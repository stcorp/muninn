#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import dictkeys, dictvalues
import os
import re
import datetime
import functools
import uuid

# Select a version of dbapi2 that's available.
# Pyspatialite is the default, thanks to legacy.
#
# These days pyspatialite is not buildable, so we actually prefer pysqlite.
# This is builtin in python as the sqlite3 module, but it may be compiled without
# extension support, in which case the user might install a homebrew version of pysqlite
# that does support it.
# In conclusion: if pyspatialite is not available, prefer homebrew versions of pysqlite over
# builtin versions.
try:
    import pyspatialite.dbapi2 as dbapi2
    _need_sqlite_extension = False
except ImportError:
    _need_sqlite_extension = True
    try:
        import pysqlite2.dbapi2 as dbapi2
    except ImportError:
        import sqlite3.dbapi2 as dbapi2

import muninn.config as config
import muninn.backends.sql as sql
import muninn.backends.blobgeometry as blobgeometry
import muninn.geometry as geometry

from muninn.exceptions import *
from muninn.function import Prototype
from muninn.schema import *
from muninn.struct import Struct


class _SQLiteConfig(Mapping):
    _alias = "sqlite"

    connection_string = Text
    mod_spatialite_path = optional(Text)
    table_prefix = optional(Text)


def create(configuration):
    options = config.parse(configuration.get("sqlite", {}), _SQLiteConfig)
    _SQLiteConfig.validate(options)
    return SQLiteBackend(**options)


class SQLiteError(Error):
    def __init__(self, message=None):
        message = "sqlite backend error" + ("" if not message else ": " + message)
        super(SQLiteError, self).__init__(message)


def translate_sqlite_errors(func):
    """Decorator that translates sqlite dbapi exceptions into muninn exceptions."""
    @functools.wraps(func)
    def translate_sqlite_errors_(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except dbapi2.Error as _error:
            try:
                message = _error.diag.message_primary
            except AttributeError:
                message = None

            if message:
                try:
                    message_detail = _error.diag.message_detail
                except AttributeError:
                    message_detail = None

                if message_detail:
                    message += " [" + message_detail + "]"
            else:
                # Remove newlines and excessive whitespace from the original SQLite exception message.
                message = " ".join(str(_error).split())

            raise SQLiteError(message)

    return translate_sqlite_errors_


def swallow_sqlite_errors(error_codes):
    """Decorator that swallows a set of specific sqlite exceptions."""
    def swallow_sqlite_errors_(func):
        @functools.wraps(func)
        def swallow_sqlite_errors__(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except dbapi2.Error as _error:
                if _error.pgcode not in error_codes:
                    raise

        return swallow_sqlite_errors__
    return swallow_sqlite_errors_


def _adapt_geometry(geometry):
    """Return the SQLite BLOB-Geometry representation of a Geometry instance."""
    return blobgeometry.encode_blob_geometry(geometry)


def _cast_geometry(blob):
    """Construct a Geometry instance from its SQLite BLOB-Geometry representation."""
    if blob is None:
        return blob
    return blobgeometry.decode_blob_geometry(blob)


class SQLiteConnection(object):
    """Wrapper for a sqlite database connection that defers (re)connection until an attempt is made to use the
    connection.

    Only non-nested transactions are supported, no auto-commit or nested transactions. A transaction can be started
    using the context manager interface.

    """
    def __init__(self, connection_string, mod_spatialite_path, backend):
        self._connection_string = connection_string
        self._mod_spatialite = mod_spatialite_path
        self._connection = None
        self._in_transaction = False
        self._backend = backend

    def close(self):
        if self._in_transaction:
            raise InternalError("unable to close the connection with the database while a transaction is in progress")

        # Close the connection.
        if self._connection is not None:
            self._disconnect()

    def cursor(self):
        if not self._in_transaction:
            raise InternalError("creating a cursor requires an active transaction")

        return self._connection.cursor()

    def __enter__(self):
        # Begin a transaction. The transaction is not started immediately, but a state change is recorded such that
        # attempts to start nested transactions can be detected. Also, the connection with the database is
        # re-established if necessary.
        #
        if self._in_transaction:
            raise InternalError("nested transactions are not supported")

        # Reconnect if necessary.
        if self._connection is None:
            self._connect()

        # Change state to guard against nested transactions.
        self._in_transaction = True

        return self

    def __exit__(self, type, value, traceback):
        # End a transaction, either by committing the transaction, or by rolling back in case an exception occured.
        if type is None:
            self._connection.commit()
        else:
            self._connection.rollback()

        self._in_transaction = False
        self.close()

    def _connect(self):
        # Re-establish the connection to the database.
        need_prepare = not os.path.exists(self._connection_string)
        self._connection = dbapi2.connect(self._connection_string, detect_types=dbapi2.PARSE_DECLTYPES)

        # make sure that foreign keys are enabled
        self._connection.execute("PRAGMA foreign_keys = ON;")

        # if we have a version of sqlite3 that support extension loading
        # load the pysqlite extension
        if _need_sqlite_extension:
            try:
                self._connection.enable_load_extension(True)
                self._connection.execute("select load_extension(\"%s\");" % (self._mod_spatialite))
            except Exception as e:
                raise Error("loading mod_spatialite extension failed (mod_spatialite_path='%s'): %s" % \
                            (self._mod_spatialite, str(e)))

        # ensure that spatial metadata init has been done
        with self._connection:
            cursor = self._connection.cursor()
            cursor.execute("PRAGMA table_info(geometry_columns);")
            if cursor.fetchall() == []:
                try:
                    cursor.execute("BEGIN")
                    cursor.execute("SELECT InitSpatialMetadata()")
                    cursor.execute("COMMIT")
                finally:
                    cursor.close()

        # create the tables if necessary
        if need_prepare:
            with self._connection:
                # we need to perform a transaction before we allow the actual transaction to happen
                self._in_transaction = True
                sqls = self._backend._create_tables_sql()
                self._backend._execute_list(sqls)
                self._connection.commit()
                self._in_transaction = False

        # Make sure TEXT data is translated to UTF-8 str types in Python (to align with default psycopg2 behavior)
        self._connection.text_factory = str

    def _disconnect(self):
        self._connection.close()
        self._connection = None

    def _get_db_type_id(self, typename):
        try:
            cursor = self._connection.cursor()
            try:
                cursor.execute("SELECT NULL::%s" % typename.lower())
                if not cursor.description:
                    raise InternalError("unable to retrieve type object id of database type: \"%s\"" % typename.upper())
                type_id = cursor.description[0][1]
            finally:
                cursor.close()
        except:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

        return type_id


class SQLiteBackend(object):
    def __init__(self, connection_string="", mod_spatialite_path="mod_spatialite", table_prefix=""):
        dbapi2.register_converter("BOOLEAN", lambda x: bool(int(x)))
        dbapi2.register_adapter(bool, lambda x: int(x))

        dbapi2.register_converter("UUID", lambda x: uuid.UUID(x.decode()))
        dbapi2.register_adapter(uuid.UUID, lambda x: x.hex)

        dbapi2.register_converter("GEOMETRY", _cast_geometry)
        dbapi2.register_adapter(geometry.Point, _adapt_geometry)
        dbapi2.register_adapter(geometry.LineString, _adapt_geometry)
        dbapi2.register_adapter(geometry.Polygon, _adapt_geometry)
        dbapi2.register_adapter(geometry.MultiPoint, _adapt_geometry)
        dbapi2.register_adapter(geometry.MultiLineString, _adapt_geometry)
        dbapi2.register_adapter(geometry.MultiPolygon, _adapt_geometry)

        self._connection_string = connection_string
        self._connection = SQLiteConnection(connection_string, mod_spatialite_path, self)

        if table_prefix and not re.match("[a-z][_a-z]*(\.[a-z][_a-z]*)*", table_prefix):
            raise ValueError("invalid table_prefix %s" % table_prefix)
        self._table_prefix = table_prefix

        self._core_table_name = self._table_name("core")
        self._link_table_name = self._table_name("link")
        self._tag_table_name = self._table_name("tag")

        self._namespace_schemas = {}
        self._sql_builder = sql.SQLBuilder({}, sql.TypeMap(), {}, self._table_name, self._placeholder,
                                           self._placeholder)

    def initialize(self, namespace_schemas):
        self._namespace_schemas = namespace_schemas
        self._sql_builder = sql.SQLBuilder(self._namespace_schemas, self._type_map(), self._rewriter_table(),
                                           self._table_name, self._placeholder, self._placeholder)

    @translate_sqlite_errors
    def disconnect(self):
        """Drop the connection to the database in order to free up resources. The connection will be re-established
        automatically when required."""
        self._connection.close()

    @translate_sqlite_errors
    def prepare(self, dry_run=False):
        result = []
        dbexists = os.path.isfile(self._connection_string)
        # If the db did not exist before, then the table creation was already done when the db was created
        if dbexists:
            sqls = self._create_tables_sql()
            if not dry_run:
                with self._connection:
                    self._execute_list(sqls)
            result = sqls
        return result

    def exists(self):
        if not os.path.isfile(self._connection_string):
            return False
        with self._connection:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name=%s" % (self._placeholder(),)
            parameters = (self._core_table_name,)
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, parameters)
                return len(cursor.fetchall()) != 0
            finally:
                cursor.close()

    @translate_sqlite_errors
    def destroy(self):
        # Each table is deleted in a separate transaction, such that the exception that is raised when a table does not
        # exist can be ignored (this requires the transaction to be rolled back).
        self._drop_tables()

    @translate_sqlite_errors
    def server_time_utc(self):
        return datetime.datetime.utcnow()

    @translate_sqlite_errors
    def insert_product_properties(self, properties):
        with self._connection:
            self._insert_namespace_properties(properties.core.uuid, "core", properties.core)
            for ns_name, ns_properties in vars(properties).items():
                if ns_name == "core":
                    continue
                self._insert_namespace_properties(properties.core.uuid, ns_name, ns_properties)

    @translate_sqlite_errors
    def update_product_properties(self, properties, uuid=None, new_namespaces=None):
        new_namespaces = new_namespaces or []
        if "core" in properties:
            self._validate_namespace_properties("core", properties.core, partial=True)
            if "uuid" in properties.core:
                uuid = properties.core.uuid if uuid is None else uuid
                if uuid != properties.core.uuid:
                    raise Error("specified uuid does not match uuid included in the specified product properties")

        if uuid is None:
            raise Error("no uuid specified and no uuid included in the specified product properties")

        with self._connection:
            self._update_namespace_properties(uuid, "core", properties.core)
            for ns_name, ns_properties in vars(properties).items():
                if ns_name == "core":
                    continue
                if ns_name in new_namespaces:
                    self._insert_namespace_properties(uuid, ns_name, ns_properties)
                else:
                    self._update_namespace_properties(uuid, ns_name, ns_properties)

    @translate_sqlite_errors
    def delete_product_properties(self, uuid):
        with self._connection:
            self._delete_product_properties(uuid)

    @translate_sqlite_errors
    def tag(self, uuid, tags):
        self._tag(uuid, tags)

    @translate_sqlite_errors
    def untag(self, uuid, tags=None):
        with self._connection:
            self._untag(uuid, tags)

    @translate_sqlite_errors
    def tags(self, uuid):
        with self._connection:
            return self._tags(uuid)

    @translate_sqlite_errors
    def link(self, uuid, source_uuids):
        self._link(uuid, source_uuids)

    @translate_sqlite_errors
    def unlink(self, uuid, source_uuids=None):
        with self._connection:
            self._unlink(uuid, source_uuids)

    @translate_sqlite_errors
    def source_products(self, uuid):
        with self._connection:
            return self._source_products(uuid)

    @translate_sqlite_errors
    def derived_products(self, uuid):
        with self._connection:
            return self._derived_products(uuid)

    @translate_sqlite_errors
    def count(self, where="", parameters={}):
        query, query_parameters = self._sql_builder.build_count_query(where, parameters)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                return cursor.fetchone()[0]
            finally:
                cursor.close()

    @translate_sqlite_errors
    def summary(self, where="", parameters={}):
        query, query_parameters = self._sql_builder.build_summary_query(where, parameters)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                row = cursor.fetchone()

                summary = Struct()
                for index, value in enumerate(row):
                    summary[cursor.description[index][0]] = value
                return summary
            finally:
                cursor.close()

    @translate_sqlite_errors
    def search(self, where="", order_by=[], limit=None, parameters={}, namespaces=[]):
        query, query_parameters, query_description = self._sql_builder.build_search_query(where, order_by, limit,
                                                                                          parameters, namespaces)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                return [self._unpack_product_properties(query_description, row) for row in cursor]
            finally:
                cursor.close()

    @translate_sqlite_errors
    def find_products_without_source(self, product_type=None, grace_period=datetime.timedelta(),
                                     force_available=False):
        """Return the core properties of all products that are not linked to any source products.

           Keyword arguments:
           product_type --  Only consider products of the specified product type.

        """
        with self._connection:
            return self._find_products_without_source(product_type, grace_period, force_available)

    @translate_sqlite_errors
    def find_products_without_available_source(self, product_type=None, grace_period=datetime.timedelta()):
        """Return the core properties of all products that are linked to one or more source products, all of which are
           unavailable. A product is unavailable if there is no data associated with it, only properties. Products that
           have links to external source products will not be selected by this function, because it cannot be determined
           whether or not these products are available.

           Keyword arguments:
           product_type --  Only consider products of the specified product type.

        """
        with self._connection:
            return self._find_products_without_available_source(product_type, grace_period)

    def _insert_namespace_properties(self, uuid, name, properties):
        self._validate_namespace_properties(name, properties)
        assert(uuid is not None and getattr(properties, "uuid", uuid) == uuid)

        # Split the properties into a list of (database) field names and a list of values. This assumes the database
        # field that corresponds to a given property has the same name. If the backend uses different field names, the
        # required translation can be performed here. Values can also be translated if necessary.
        properties_dict = vars(properties)
        fields, parameters = dictkeys(properties_dict), dictvalues(properties_dict)
        # Ensure the uuid field is present (for namespaces other than the core namespace this is used as the foreign
        # key).
        if "uuid" not in properties:
            fields.append("uuid")
            parameters.append(uuid)

        # Build and execute INSERT query.
        query = "INSERT INTO %s (%s) VALUES (%s)" % (self._table_name(name), ", ".join(fields),
                                                     ", ".join([self._placeholder()] * len(fields)))

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
        finally:
            cursor.close()

    def _update_namespace_properties(self, uuid, name, properties):
        self._validate_namespace_properties(name, properties, partial=True)
        assert(uuid is not None and getattr(properties, "uuid", uuid) == uuid)

        # Split the properties into a list of (database) field names and a list of values. This assumes the database
        # field that corresponds to a given property has the same name. If the backend uses different field names, the
        # required translation can be performed here. Values can also be translated if necessary.
        properties_dict = vars(properties)
        fields, parameters = dictkeys(properties_dict), dictvalues(properties_dict)
        if not fields:
            return  # nothing to do

        # Remove the uuid field if present. This field needs to be included in the WHERE clause of the UPDATE query, not
        # in the SET clause.
        try:
            uuid_index = fields.index("uuid")
        except ValueError:
            pass
        else:
            del fields[uuid_index]
            del parameters[uuid_index]

        # Append the uuid (value) at the end of the list of parameters (will be used in the WHERE clause).
        parameters.append(uuid)

        # Build and execute UPDATE query.
        set_clause = ", ".join(["%s = %s" % (field, self._placeholder()) for field in fields])
        query = "UPDATE %s SET %s WHERE uuid = %s" % (self._table_name(name), set_clause, self._placeholder())

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            assert(cursor.rowcount <= 1)

            if cursor.rowcount != 1:
                raise Error("could not update properties for namespace: %s for product: %s" % (name, uuid))
        finally:
            cursor.close()

    def _delete_product_properties(self, uuid):
        cursor = self._connection.cursor()
        try:
            cursor.execute("DELETE FROM %s WHERE source_uuid = %s" % (self._link_table_name, self._placeholder()),
                           (uuid,))
            cursor.execute("DELETE FROM %s WHERE uuid = %s" % (self._core_table_name, self._placeholder()), (uuid,))
            assert(cursor.rowcount <= 1)

            if cursor.rowcount != 1:
                raise Error("could not delete properties for product: %s" % (uuid,))
        finally:
            cursor.close()

    def _tag(self, uuid, tags):
        query = "INSERT INTO %s (uuid, tag) VALUES (%s, %s)" % (self._tag_table_name, self._placeholder(),
                                                                self._placeholder())
        for tag in tags:
            with self._connection:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(query, (uuid, tag))
                except dbapi2.Error as _error:
                    # If the tag already exists, swallow the exception.
                    if not _error.message.endswith('not unique'):
                        raise
                finally:
                    cursor.close()

    def _untag(self, uuid, tags=None):
        if tags is None:
            query = "DELETE FROM %s WHERE uuid = %s" % (self._tag_table_name, self._placeholder())
            parameters = (uuid,)
        else:
            query = "DELETE FROM %s WHERE uuid = %s AND tag IN (%s)" % \
                    (self._tag_table_name, self._placeholder(), ','.join(self._placeholder() * len(tags)))
            parameters = [uuid] + list(tags)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
        finally:
            cursor.close()

    def _tags(self, uuid):
        query = "SELECT tag FROM %s WHERE uuid = %s ORDER BY tag" % (self._tag_table_name, self._placeholder())
        parameters = (uuid,)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            return [row[0] for row in cursor]
        finally:
            cursor.close()

    def _link(self, uuid, source_uuids):
        query = "INSERT INTO %s (uuid, source_uuid) VALUES (%s, %s)" % (self._link_table_name, self._placeholder(),
                                                                        self._placeholder())

        for source_uuid in source_uuids:
            with self._connection:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(query, (uuid, source_uuid))
                except dbapi2.Error as _error:
                    # If the link already exists, swallow the exception.
                    if not _error.message.endswith('not unique'):
                        raise
                finally:
                    cursor.close()

    def _unlink(self, uuid, source_uuids=None):
        if source_uuids is None:
            query = "DELETE FROM %s WHERE uuid = %s" % (self._link_table_name, self._placeholder())
            parameters = (uuid,)
        else:
            query = "DELETE FROM %s WHERE uuid = %s AND source_uuid IN (%s)" % \
                (self._link_table_name, self._placeholder(), ','.join(self._placeholder() * len(tags)))
            parameters = [uuid] + list(source_uuids)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
        finally:
            cursor.close()

    def _source_products(self, uuid):
        query = "SELECT source_uuid FROM %s WHERE uuid = %s" % (self._link_table_name, self._placeholder())
        parameters = (uuid,)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            return [row[0] for row in cursor]
        finally:
            cursor.close()

    def _derived_products(self, uuid):
        query = "SELECT uuid FROM %s WHERE source_uuid = %s" % (self._link_table_name, self._placeholder())
        parameters = (uuid,)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            return [row[0] for row in cursor]
        finally:
            cursor.close()

    def _find_products_without_source(self, product_type=None, grace_period=datetime.timedelta(),
                                      force_available=False):
        core_properties = list(self._namespace_schema("core"))
        select_list = ["%s.%s" % (self._core_table_name, name) for name in core_properties]
        query = "SELECT %s FROM %s WHERE %s.active AND datetime(\"now\") - %s.archive_date > %s AND NOT " \
                "EXISTS (SELECT 1 FROM %s WHERE %s.uuid = %s.uuid)" % (", ".join(select_list),
                                                                       self._core_table_name, self._core_table_name,
                                                                       self._core_table_name, self._placeholder(),
                                                                       self._link_table_name, self._link_table_name,
                                                                       self._core_table_name)

        if product_type is not None:
            query = "%s AND product_type = %s" % (query, self._placeholder())

        if force_available:
            query = "%s AND archive_path IS NOT NULL" % query

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, (grace_period,) if product_type is None else (grace_period, product_type))

            description = [("core", core_properties)]
            return [self._unpack_product_properties(description, row) for row in cursor]
        finally:
            cursor.close()

    def _find_products_without_available_source(self, product_type=None, grace_period=datetime.timedelta()):
        core_properties = list(self._namespace_schema("core"))
        select_list = ["%s.%s" % (self._core_table_name, name) for name in core_properties]

        query = "SELECT %s FROM %s WHERE active AND datetime(\"now\") - archive_date > %s AND uuid IN (SELECT " \
                "uuid FROM %s EXCEPT SELECT DISTINCT link.uuid FROM %s AS link LEFT JOIN %s AS source ON " \
                "(link.source_uuid = source.uuid) WHERE source.uuid IS NULL OR source.archive_path IS NOT NULL)" % \
                (", ".join(select_list), self._core_table_name, self._placeholder(), self._link_table_name,
                 self._link_table_name, self._core_table_name)

        if product_type is not None:
            query = "%s AND product_type = %s" % (query, self._placeholder())

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, (grace_period,) if product_type is None else (grace_period, product_type))

            description = [("core", core_properties)]
            return [self._unpack_product_properties(description, row) for row in cursor]
        finally:
            cursor.close()

    def _unpack_product_properties(self, description, values):
        unpacked_properties, start = Struct(), 0
        for ns_name, ns_description in description:
            end = start + len(ns_description)

            # A value of None for the uuid field of namespaces other than the core namespace indicates the namespace is
            # not defined for the product under consideration. In this case, the entire namespace will be skipped.
            # Otherwise, only the uuid field is skipped, as it is an implementation detail (foreign key) and it is not
            # part of the namespace itself.
            #
            if ns_name != "core":
                assert(ns_description[0] == "uuid")
                if values[start] is None:
                    # Skip the entire namespace.
                    start = end
                    continue

                # Skip the uuid field.
                start += 1
                ns_description = ns_description[1:]

            unpacked_ns_properties = self._unpack_namespace_properties(ns_name, ns_description, values[start:end])
            self._validate_namespace_properties(ns_name, unpacked_ns_properties)
            unpacked_properties[ns_name] = unpacked_ns_properties
            start = end

        return unpacked_properties

    def _unpack_namespace_properties(self, namespace, description, values):
        unpacked_properties = Struct()
        schema = self._namespace_schema(namespace)
        for property, value in zip(description, values):
            if value is not None or not schema.is_optional(property):
                unpacked_properties[property] = value
        return unpacked_properties

    def _validate_namespace_properties(self, namespace, properties, partial=False):
        self._namespace_schema(namespace).validate(properties, partial)

    def _execute_list(self, sql_list):
        cursor = self._connection.cursor()
        try:
            for sql in sql_list:
                cursor.execute(sql)
        finally:
            cursor.close()

    def _create_tables_sql(self):
        result = []
        # Create the table for the core namespace.
        column_sql = []
        schema = self._namespace_schema("core")
        for name in schema:
            type_name = self._type_map()[schema[name]]
            if type_name != "GEOMETRY":
                sql = name + " " + type_name
                if not schema.is_optional(name):
                    sql = sql + " " + "NOT NULL"
                column_sql.append(sql)
        column_sql.append("PRIMARY KEY (uuid)")
        column_sql.append("UNIQUE (archive_path, physical_name)")
        column_sql.append("UNIQUE (product_type, product_name)")
        result.append("CREATE TABLE %s (%s)" % (self._core_table_name, ", ".join(column_sql)))
        for name in schema:
            if self._type_map()[schema[name]] == "GEOMETRY":
                result.append("SELECT AddGeometryColumn('%s', '%s', 4326, 'GEOMETRY', 2)" %
                               (self._core_table_name, name,))
        for field in ['active', 'hash', 'size', 'archive_date', 'product_type', 'product_name', 'physical_name',
                      'validity_start', 'validity_stop', 'creation_date']:
            result.append("CREATE INDEX idx_%s_%s ON %s (%s)"
                           % (self._core_table_name, field, self._core_table_name, field))

        # For the geospatial footprint we need to use a special spatial index
        result.append("SELECT CreateSpatialIndex('%s', '%s')" % (self._core_table_name, 'footprint'))

        # Create the tables for all non-core namespaces.
        for namespace in self._namespace_schemas:
            if namespace == "core":
                continue

            column_sql = []
            schema = self._namespace_schema(namespace)
            for name in schema:
                type_name = self._type_map()[schema[name]]
                if type_name != "GEOMETRY":
                    sql = name + " " + type_name
                    if not schema.is_optional(name):
                        sql = sql + " " + "NOT NULL"
                    column_sql.append(sql)
            column_sql.append("uuid UUID PRIMARY KEY REFERENCES %s(uuid) ON DELETE CASCADE" %
                              self._core_table_name)
            result.append("CREATE TABLE %s (%s)" % (self._table_name(namespace), ", ".join(column_sql)))
            for name in schema:
                if self._type_map()[schema[name]] == "GEOMETRY":
                    result.append("SELECT AddGeometryColumn('%s', '%s', 4326, 'GEOMETRY', 2)" %
                                   (self._table_name(namespace), name))

        # We use explicit 'id' primary keys for the links and tags tables so the entries can be managed using
        # other front-ends that may not support tuples as primary keys.

        # Create the table for links.
        result.append("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, uuid UUID REFERENCES %s(uuid) ON DELETE CASCADE, "
                       "source_uuid UUID NOT NULL, UNIQUE (uuid, source_uuid))" %
                       (self._link_table_name, self._core_table_name))
        result.append("CREATE INDEX idx_%s_uuid ON %s (uuid)"
                       % (self._link_table_name, self._link_table_name))
        result.append("CREATE INDEX idx_%s_source_uuid ON %s (source_uuid)"
                       % (self._link_table_name, self._link_table_name))

        # Create the table for tags.
        result.append("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, uuid UUID REFERENCES %s(uuid) ON DELETE CASCADE, "
                       "tag TEXT NOT NULL, UNIQUE (uuid, tag))" % (self._tag_table_name, self._core_table_name))
        result.append("CREATE INDEX idx_%s_uuid ON %s (uuid)"
                       % (self._tag_table_name, self._tag_table_name))
        result.append("CREATE INDEX idx_%s_tag ON %s (tag)"
                       % (self._tag_table_name, self._tag_table_name))
        return result

    def _drop_tables(self):
        with self._connection:
            cursor = self._connection.cursor()
            try:
                # remove geospatial footprint index
                cursor.execute("SELECT DisableSpatialIndex('%s', '%s')" % (self._core_table_name, 'footprint'))
                cursor.execute("DROP TABLE IF EXISTS idx_%s_footprint" % self._core_table_name)
                # first remote all links to geometry columns
                for namespace in self._namespace_schemas:
                    schema = self._namespace_schema(namespace)
                    for name in schema:
                        if self._type_map()[schema[name]] == "GEOMETRY":
                            cursor.execute("SELECT DiscardGeometryColumn('%s', '%s')" % (self._core_table_name, name))
                # then remove the tables
                cursor.execute("DROP TABLE IF EXISTS %s" % self._tag_table_name)
                cursor.execute("DROP TABLE IF EXISTS %s" % self._link_table_name)
                for namespace in self._namespace_schemas:
                    if namespace != "core":
                        cursor.execute("DROP TABLE IF EXISTS %s" % self._table_name(namespace))
                # remove 'core' table last
                if "core" in self._namespace_schemas:
                    cursor.execute("DROP TABLE IF EXISTS %s" % self._core_table_name)
            finally:
                cursor.close()

    def _namespace_schema(self, namespace):
        try:
            return self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"" % namespace)

    def _table_name(self, name):
        return name if not self._table_prefix else self._table_prefix + name

    def _placeholder(self, name=""):
        return "?" if not name else ":%s" % name

    def _type_map(self):
        type_map = sql.TypeMap()
        type_map[Long] = "INTEGER"
        type_map[Integer] = "INTEGER"
        type_map[Real] = "REAL"
        type_map[Boolean] = "BOOLEAN"
        type_map[Text] = "TEXT"
        type_map[Timestamp] = "TIMESTAMP"
        type_map[UUID] = "UUID"
        type_map[Geometry] = "GEOMETRY"

        return type_map

    def _rewriter_table(self):
        rewriter_table = sql.default_rewriter_table()

        #
        # Timestamp binary minus operator.
        #
        rewriter_table[Prototype("-", (Timestamp, Timestamp), Real)] = \
            lambda arg0, arg1: "(julianday(%s) - julianday(%s)) * 86400.0" % (arg0, arg1)

        #
        # Enable escape sequences with the LIKE operator
        #
        rewriter_table[Prototype("~=", (Text, Text), Boolean)] = \
            lambda arg0, arg1: "(%s) LIKE (%s) ESCAPE '\\'" % (arg0, arg1)

        #
        # Functions.
        #
        rewriter_table[Prototype("covers", (Geometry, Geometry), Boolean)] = \
            sql.binary_function_rewriter("ST_Covers")

        rewriter_table[Prototype("intersects", (Geometry, Geometry), Boolean)] = \
            sql.binary_function_rewriter("ST_Intersects")

        rewriter_table[Prototype("is_source_of", (UUID,), Boolean)] = \
            lambda arg0: "EXISTS (SELECT 1 FROM %s WHERE source_uuid = %s.uuid AND uuid = (%s))" % \
            (self._link_table_name, self._core_table_name, arg0)

        rewriter_table[Prototype("is_derived_from", (UUID,), Boolean)] = \
            lambda arg0: "EXISTS (SELECT 1 FROM %s WHERE uuid = %s.uuid AND source_uuid = (%s))" % \
            (self._link_table_name, self._core_table_name, arg0)

        rewriter_table[Prototype("has_tag", (Text,), Boolean)] = \
            lambda arg0: "EXISTS (SELECT 1 FROM %s WHERE uuid = %s.uuid AND tag = (%s))" % \
            (self._tag_table_name, self._core_table_name, arg0)

        rewriter_table[Prototype("now", (), Timestamp)] = \
            sql.as_is("datetime(\"now\")")

        return rewriter_table
