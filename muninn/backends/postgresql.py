#
# Copyright (C) 2014-2019 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import dictkeys, dictvalues, is_python2_unicode

import re
import functools
import psycopg2
import psycopg2.errorcodes
import psycopg2.extensions
import psycopg2.extras

import muninn.config as config
import muninn.backends.sql as sql
import muninn.backends.ewkb as ewkb
import muninn.geometry as geometry

from muninn.exceptions import *
from muninn.function import Prototype
from muninn.schema import *
from muninn.struct import Struct


class _PostgresqlConfig(Mapping):
    _alias = "postgresql"

    connection_string = Text
    table_prefix = optional(Text)


def create(configuration):
    options = config.parse(configuration.get("postgresql", {}), _PostgresqlConfig)
    _PostgresqlConfig.validate(options)
    return PostgresqlBackend(**options)


class PostgresqlError(Error):
    def __init__(self, message=None):
        message = "postgresql backend error" + ("" if not message else ": " + message)
        super(PostgresqlError, self).__init__(message)


def translate_psycopg_errors(func):
    """Decorator that translates psycopg2 exceptions into muninn exceptions."""
    @functools.wraps(func)
    def translate_psycopg_errors_(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.Error as _error:
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
                # Remove newlines and excessive whitespace from the original Postgresql exception message.
                message = " ".join(str(_error).split())

            raise PostgresqlError(message)

    return translate_psycopg_errors_


def swallow_psycopg2_errors(error_codes):
    """Decorator that swallows a set of specific psycopg2 exceptions."""
    def swallow_psycopg2_errors_(func):
        @functools.wraps(func)
        def swallow_psycopg2_errors__(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except psycopg2.Error as _error:
                if _error.pgcode not in error_codes:
                    raise

        return swallow_psycopg2_errors__
    return swallow_psycopg2_errors_


def _adapt_geometry(geometry):
    """Return the hexadecimal extended well known binary format (hexewkb) representation of a Geometry instance."""
    return psycopg2.extensions.AsIs("'%s'" % ewkb.encode_hexewkb(geometry))


def _cast_geography(hexewkb, cursor):
    """Construct a Geometry instance from its hexadecimal extended well known binary format (hexewkb) representation."""
    if hexewkb is None:
        return hexewkb
    return ewkb.decode_hexewkb(hexewkb)


class PostgresqlConnection(object):
    """Wrapper for a psycopg2 database connection that defers (re)connection until an attempt is made to use the
    connection.

    Only non-nested transactions are supported, no auto-commit or nested transactions. A transaction can be started
    using the context manager interface.

    """
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self._connection = None
        self._in_transaction = False

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
        try:
            if type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._in_transaction = False
            self.close()

    def _connect(self):
        # Re-establish the connection to the database.
        self._connection = psycopg2.connect(self._connection_string)

        # Register adapter and cast for the UUID type.
        psycopg2.extras.register_uuid(conn_or_curs=self._connection)

        # Register adapter for the Geometry type.
        psycopg2.extensions.register_adapter(geometry.Geometry, _adapt_geometry)

        # Register cast for the Geometry type.
        geography_oid = self._get_db_type_id("geography")
        geography_type = psycopg2.extensions.new_type((geography_oid,), "GEOGRAPHY", _cast_geography)
        psycopg2.extensions.register_type(geography_type, self._connection)

    def _disconnect(self):
        self._connection.close()
        self._connection = None

    @property
    def encoding(self):
        return self._connection.encoding

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


class PostgresqlBackend(object):
    def __init__(self, connection_string="", table_prefix=""):
        self._connection = PostgresqlConnection(connection_string)

        if table_prefix and not re.match("[a-z][_a-z]*(\.[a-z][_a-z]*)*", table_prefix):
            raise ValueError("invalid table_prefix %s" % table_prefix)
        self._table_prefix = table_prefix

        self._core_table_name = self._table_name("core")
        self._link_table_name = self._table_name("link")
        self._tag_table_name = self._table_name("tag")

        self._namespace_schemas = {}
        self._sql_builder = sql.SQLBuilder({}, sql.TypeMap(), {}, self._table_name, self._placeholder,
                                           self._placeholder, self._rewriter_property)

    def initialize(self, namespace_schemas):
        self._namespace_schemas = namespace_schemas
        self._sql_builder = sql.SQLBuilder(self._namespace_schemas, self._type_map(), self._rewriter_table(),
                                           self._table_name, self._placeholder, self._placeholder, self._rewriter_property)

    @translate_psycopg_errors
    def disconnect(self):
        """Drop the connection to the database in order to free up resources. The connection will be re-established
        automatically when required."""
        self._connection.close()

    @translate_psycopg_errors
    def prepare(self, dry_run=False):
        sqls = self._create_tables_sql()
        if not dry_run:
            with self._connection:
                self._execute_list(sqls)
        return sqls

    @translate_psycopg_errors
    def exists(self):
        with self._connection:
            query = "SELECT relname FROM pg_class WHERE relname=%s" % (self._placeholder(),)
            parameters = (self._core_table_name,)
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, parameters)
                return len(cursor.fetchall()) != 0
            finally:
                cursor.close()

    @translate_psycopg_errors
    def destroy(self):
        self._drop_tables()

    @translate_psycopg_errors
    def server_time_utc(self):
        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute("SELECT timezone(%s, now())", ("UTC",))
                assert(cursor.rowcount == 1)
                return cursor.fetchone()[0]
            finally:
                cursor.close()

    @translate_psycopg_errors
    def insert_product_properties(self, properties):
        with self._connection:
            self._insert_namespace_properties(properties.core.uuid, "core", properties.core)
            for ns_name, ns_properties in vars(properties).items():
                if ns_name == "core":
                    continue
                self._insert_namespace_properties(properties.core.uuid, ns_name, ns_properties)

    @translate_psycopg_errors
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

    @translate_psycopg_errors
    def delete_product_properties(self, uuid):
        with self._connection:
            self._delete_product_properties(uuid)

    @translate_psycopg_errors
    def tag(self, uuid, tags):
        self._tag(uuid, tags)

    @translate_psycopg_errors
    def untag(self, uuid, tags=None):
        with self._connection:
            self._untag(uuid, tags)

    @translate_psycopg_errors
    def tags(self, uuid):
        with self._connection:
            return self._tags(uuid)

    @translate_psycopg_errors
    def link(self, uuid, source_uuids):
        self._link(uuid, source_uuids)

    @translate_psycopg_errors
    def unlink(self, uuid, source_uuids=None):
        with self._connection:
            self._unlink(uuid, source_uuids)

    @translate_psycopg_errors
    def source_products(self, uuid):
        with self._connection:
            return self._source_products(uuid)

    @translate_psycopg_errors
    def derived_products(self, uuid):
        with self._connection:
            return self._derived_products(uuid)

    @translate_psycopg_errors
    def count(self, where="", parameters={}):
        query, query_parameters = self._sql_builder.build_count_query(where, parameters)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                assert(cursor.rowcount == 1)
                return cursor.fetchone()[0]
            finally:
                cursor.close()

    @translate_psycopg_errors
    def summary(self, where="", parameters=None, aggregates=None, group_by=None, group_by_tag=False, order_by=None):
        query, query_parameters, query_description = self._sql_builder.build_summary_query(
            where, parameters, aggregates, group_by, group_by_tag, order_by)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                return [row for row in cursor], query_description
            finally:
                cursor.close()

    @translate_psycopg_errors
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

    @translate_psycopg_errors
    def find_products_without_source(self, product_type=None, grace_period=datetime.timedelta(),
                                     archived_only=False):
        """Return the core properties of all products that are not linked to any source products.

           Keyword arguments:
           product_type --  Only consider products of the specified product type.

        """
        with self._connection:
            return self._find_products_without_source(product_type, grace_period, archived_only)

    @translate_psycopg_errors
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
        query = "INSERT INTO %s (uuid, tag) SELECT %s, %s" % (self._tag_table_name, self._placeholder(),
                                                                self._placeholder())
        query += " WHERE NOT EXISTS (SELECT 1 FROM %s WHERE uuid=%s and tag=%s)" % (self._tag_table_name,
                                                                                    self._placeholder(),
                                                                                    self._placeholder())
        for tag in tags:
            with self._connection:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(query, (uuid, tag, uuid, tag))
                except psycopg2.Error as _error:
                    # There is still a small chance due to concurrency that the tag already exists.
                    # For those cases we swallow the exception.
                    if _error.pgcode != psycopg2.errorcodes.UNIQUE_VIOLATION:
                        raise
                finally:
                    cursor.close()

    def _untag(self, uuid, tags=None):
        if tags is None:
            query = "DELETE FROM %s WHERE uuid = %s" % (self._tag_table_name, self._placeholder())
            parameters = (uuid,)
        else:
            query = "DELETE FROM %s WHERE uuid = %s AND tag IN %s" % (self._tag_table_name, self._placeholder(),
                                                                      self._placeholder())
            parameters = (uuid, tuple(tags))

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
        query = "INSERT INTO %s (uuid, source_uuid) SELECT %s, %s" % (self._link_table_name, self._placeholder(),
                                                                        self._placeholder())

        query += " WHERE NOT EXISTS (SELECT 1 FROM %s WHERE uuid=%s and source_uuid=%s)" % (self._link_table_name,
                                                                                            self._placeholder(),
                                                                                            self._placeholder())
        for source_uuid in source_uuids:
            with self._connection:
                cursor = self._connection.cursor()
                try:
                    cursor.execute(query, (uuid, source_uuid, uuid, source_uuid))
                except psycopg2.Error as _error:
                    # There is still a small chance due to concurrency that the link already exists.
                    # For those cases we swallow the exception.
                    if _error.pgcode != psycopg2.errorcodes.UNIQUE_VIOLATION:
                        raise
                finally:
                    cursor.close()

    def _unlink(self, uuid, source_uuids=None):
        if source_uuids is None:
            query = "DELETE FROM %s WHERE uuid = %s" % (self._link_table_name, self._placeholder())
            parameters = (uuid,)
        else:
            query = "DELETE FROM %s WHERE uuid = %s AND source_uuid IN %s" % (self._link_table_name,
                                                                              self._placeholder(), self._placeholder())
            parameters = (uuid, tuple(source_uuids))

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
                                      archived_only=False):
        core_properties = list(self._namespace_schema("core"))
        select_list = ["%s.%s" % (self._core_table_name, name) for name in core_properties]
        query = "SELECT %s FROM %s WHERE %s.active AND now() AT TIME ZONE 'UTC' - %s.archive_date > %s AND NOT " \
                "EXISTS (SELECT 1 FROM %s WHERE %s.uuid = %s.uuid)" % (", ".join(select_list),
                                                                       self._core_table_name, self._core_table_name,
                                                                       self._core_table_name, self._placeholder(),
                                                                       self._link_table_name, self._link_table_name,
                                                                       self._core_table_name)

        if product_type is not None:
            query = "%s AND product_type = %s" % (query, self._placeholder())

        if archived_only:
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

        query = "SELECT %s FROM %s WHERE active AND now() AT TIME ZONE 'UTC' - archive_date > %s AND uuid IN (SELECT " \
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
            # We may get unicode from the psycopg2 connection
            # if, possibly by a third party, the UNICODE adapter is loaded.
            # Muninn assumes strs
            if is_python2_unicode(value):
                value = value.encode(self._connection.encoding)

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
        result.append(self._sql_builder.build_create_table_query("core"))
        result.append("ALTER TABLE %s ADD PRIMARY KEY (uuid)" % self._core_table_name)
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_archive_path_uniq UNIQUE (archive_path, physical_name)"
                       % (self._core_table_name, self._core_table_name))
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_product_name_uniq UNIQUE (product_type, product_name)"
                       % (self._core_table_name, self._core_table_name))
        for field in ['active', 'hash', 'size', 'archive_date', 'product_type', 'product_name', 'physical_name',
                      'validity_start', 'validity_stop', 'creation_date']:
            result.append("CREATE INDEX idx_%s_%s ON %s (%s)"
                           % (self._core_table_name, field, self._core_table_name, field))

        # For the geospatial footprint we need to use a special GIST index
        result.append("CREATE INDEX idx_%s_footprint ON %s USING GIST (footprint)"
                       % (self._core_table_name, self._core_table_name))

        # Create the tables for all non-core namespaces.
        for namespace in self._namespace_schemas:
            if namespace == "core":
                continue

            result.append(self._sql_builder.build_create_table_query(namespace))

            result.append("ALTER TABLE %s ADD COLUMN uuid UUID PRIMARY KEY" % self._table_name(namespace))
            result.append("ALTER TABLE %s ADD CONSTRAINT %s_uuid_fkey FOREIGN KEY (uuid) REFERENCES %s (uuid) ON "
                           "DELETE CASCADE" % (self._table_name(namespace), self._table_name(namespace),
                                               self._core_table_name))

        # We use explicit 'id' primary keys for the links and tags tables so the entries can be managed using
        # other front-ends that may not support tuples as primary keys.

        # Create the table for links.
        result.append("CREATE TABLE %s (id SERIAL PRIMARY KEY, uuid UUID NOT NULL, source_uuid UUID NOT NULL)"
                       % self._link_table_name)
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_link_uuid_source_uuid_uniq UNIQUE (uuid, source_uuid)"
                       % (self._link_table_name, self._link_table_name))
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_uuid_fkey FOREIGN KEY (uuid) REFERENCES %s (uuid) ON "
                       "DELETE CASCADE" % (self._link_table_name, self._link_table_name,
                                           self._core_table_name))
        result.append("CREATE INDEX idx_%s_uuid ON %s (uuid)"
                       % (self._link_table_name, self._link_table_name))
        result.append("CREATE INDEX idx_%s_source_uuid ON %s (source_uuid)"
                       % (self._link_table_name, self._link_table_name))

        # Create the table for tags.
        result.append("CREATE TABLE %s (id SERIAL PRIMARY KEY, uuid UUID NOT NULL, tag TEXT NOT NULL)"
                       % self._tag_table_name)
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_tag_uuid_tag_uniq UNIQUE (uuid, tag)"
                       % (self._tag_table_name, self._tag_table_name))
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_uuid_fkey FOREIGN KEY (uuid) REFERENCES %s (uuid) ON "
                       "DELETE CASCADE" % (self._tag_table_name, self._tag_table_name,
                                           self._core_table_name))
        result.append("CREATE INDEX idx_%s_uuid ON %s (uuid)"
                       % (self._tag_table_name, self._tag_table_name))
        result.append("CREATE INDEX idx_%s_tag ON %s (tag)"
                       % (self._tag_table_name, self._tag_table_name))
        return result

    def _drop_tables(self):
        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS %s CASCADE" % self._tag_table_name)
                cursor.execute("DROP TABLE IF EXISTS %s CASCADE" % self._link_table_name)
                for namespace in self._namespace_schemas:
                    if namespace != "core":
                        cursor.execute("DROP TABLE IF EXISTS %s CASCADE" % self._table_name(namespace))
                # remove 'core' table last
                if "core" in self._namespace_schemas:
                    cursor.execute("DROP TABLE IF EXISTS %s CASCADE" % self._core_table_name)
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
        return "%s" if not name else "%%(%s)s" % name

    def _type_map(self):
        type_map = sql.TypeMap()
        type_map[Long] = "BIGINT"
        type_map[Integer] = "INTEGER"
        type_map[Real] = "DOUBLE PRECISION"
        type_map[Boolean] = "BOOLEAN"
        type_map[Text] = "TEXT"
        type_map[Timestamp] = "TIMESTAMP"
        type_map[UUID] = "UUID"
        type_map[Geometry] = "GEOGRAPHY"

        return type_map

    def _rewriter_table(self):
        rewriter_table = sql.default_rewriter_table()

        #
        # Timestamp binary minus operator.
        #
        rewriter_table[Prototype("-", (Timestamp, Timestamp), Real)] = \
            lambda arg0, arg1: "EXTRACT(EPOCH FROM (%s) - (%s))" % (arg0, arg1)

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
            sql.as_is("now() AT TIME ZONE 'UTC'")

        return rewriter_table

    def _rewriter_property(self, column_name, subscript):
        # timestamp
        if subscript == 'year':
            return "TO_CHAR(%s, 'YYYY')" % column_name
        if subscript == 'month':
            return "TO_CHAR(%s, 'MM')" % column_name
        if subscript == 'yearmonth':
            return "TO_CHAR(%s, 'YYYY-MM')" % column_name
        if subscript == 'date':
            return "TO_CHAR(%s, 'YYYY-MM-DD')" % column_name
        # text
        if subscript == 'length':
            return "CHAR_LENGTH(%s)" % column_name
        raise ValueError('Unsupported subscript: %s' % subscript)
