#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import dictkeys, dictvalues, is_python2_unicode

import re
import functools
import inspect
import json

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
except ImportError:
    pass

try:
    import pg8000
    pg8000.paramstyle = 'pyformat'
except ImportError:
    pass

import muninn.config as config
import muninn.database.sql as sql
import muninn.database.ewkb as ewkb
import muninn.geometry as geometry

from muninn.exceptions import *
from muninn.function import Prototype
from muninn.schema import *
from muninn.struct import Struct

PG_UNIQUE_VIOLATION = '23505'


def _get_db_type_id(connection, typename):
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT NULL::%s" % typename.lower())
            if not cursor.description:
                raise InternalError("unable to retrieve type object id of database type: \"%s\"" % typename.upper())
            type_id = cursor.description[0][1]
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()

    return type_id


def geometry_recv(data, offset, length):  # TODO binary send/recv needed for pg8000 <= 1.15
    return ewkb.decode_ewkb(data[offset:offset+length])


def geometry_recv_hex(data):
    return ewkb.decode_hexewkb(data)


def geometry_send(geometry):
    return ewkb.encode_ewkb(geometry)


def geometry_send_hex(geometry):
    return ewkb.encode_hexewkb(geometry)


def _connect_pg8000(connection_string):
    kwargs = dict(arg.split('=') for arg in connection_string.split(' '))
    _connection = pg8000.connect(**kwargs)

    geography_oid = _get_db_type_id(_connection, "geography")

    if hasattr(_connection, 'register_in_adapter'):
        _connection.register_in_adapter(geography_oid, geometry_recv_hex)
    else:
        _connection.pg_types[geography_oid] = (pg8000.core.FC_BINARY, geometry_recv)

    for type_ in (
        geometry.Point,
        geometry.Polygon,
        geometry.LineString,
        geometry.MultiPoint,
        geometry.MultiPolygon,
        geometry.MultiLineString,
    ):
        if hasattr(_connection, 'register_out_adapter'):
            try:
                getargspec = inspect.getfullargspec
            except AttributeError:
                getargspec = inspect.getargspec
            # pg8000 removed the oid argument in v1.22
            if len(getargspec(_connection.register_out_adapter).args) == 3:
                _connection.register_out_adapter(type_, geometry_send_hex)
            else:
                _connection.register_out_adapter(type_, geography_oid, geometry_send_hex)
        else:
            _connection.py_types[type_] = (geography_oid, pg8000.core.FC_BINARY, geometry_send)

    return _connection


def _adapt_geometry(geometry):
    """Return the hexadecimal extended well known binary format (hexewkb) representation of a Geometry instance."""
    return psycopg2.extensions.AsIs("'%s'" % ewkb.encode_hexewkb(geometry))


def _cast_geography(hexewkb, cursor):
    """Construct a Geometry instance from its hexadecimal extended well known binary format (hexewkb) representation."""
    if hexewkb is None:
        return hexewkb
    return ewkb.decode_hexewkb(hexewkb)


def _connect_psycopg2(connection_string):
    _connection = psycopg2.connect(connection_string)

    # Register adapter and cast for the UUID type.
    psycopg2.extras.register_uuid(conn_or_curs=_connection)

    # Register adapter for the Geometry type.
    psycopg2.extensions.register_adapter(geometry.Geometry, _adapt_geometry)

    # Register cast for the Geometry type.
    geography_oid = _get_db_type_id(_connection, "geography")
    geography_type = psycopg2.extensions.new_type((geography_oid,), "GEOGRAPHY", _cast_geography)
    psycopg2.extensions.register_type(geography_type, _connection)

    return _connection


class _PostgresqlConfig(Mapping):
    _alias = "postgresql"

    library = Text(optional=True)
    connection_string = Text()
    table_prefix = Text(optional=True)


def create(configuration):
    options = config.parse(configuration.get("postgresql", {}), _PostgresqlConfig)
    _PostgresqlConfig.validate(options)
    return PostgresqlBackend(**options)


class PostgresqlError(Error):
    def __init__(self, message=None):
        message = "postgresql backend error" + ("" if not message else ": " + message)
        super(PostgresqlError, self).__init__(message)


def translate_errors(func):
    """Decorator that translates db 2.0 api exceptions into muninn exceptions."""
    @functools.wraps(func)
    def translate_errors_(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except self._connection._backend.Error as _error:
            message = None

            # psycopg2
            if self._library == 'psycopg2':
                try:
                    message = _error.diag.message_primary
                    if message is not None:
                        try:
                            message_detail = _error.diag.message_detail
                            if message_detail:
                                message += " [" + message_detail + "]"
                        except AttributeError:
                            pass
                except AttributeError:
                    pass

            elif self._library == 'pg8000':
                try:
                    message = _error.args[0]['M']
                except (TypeError, IndexError, AttributeError, KeyError):
                    pass

            # fallback
            if message is None:
                message = ' '.join(str(_error).split())

            raise PostgresqlError(message)

    return translate_errors_


# TODO use sub-classes for psycopg2, pg8000

class PostgresqlConnection(object):
    """Wrapper for a database connection that defers (re)connection until an attempt is made to use the
    connection.

    Only non-nested transactions are supported, no auto-commit or nested transactions. A transaction can be started
    using the context manager interface.

    """
    def __init__(self, connection_string, library):
        self._connection_string = connection_string
        self._library = library
        self._connection = None
        self._in_transaction = False

        if library == 'psycopg2':
            try:
                self._backend = psycopg2
            except NameError:
                raise Error('could not import psycopg2')

        elif library == 'pg8000':
            try:
                self._backend = pg8000
            except NameError:
                raise Error('could not import pg8000')
        else:
            raise Error('no such library: %s' % library)

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
        if self._library == 'psycopg2':
            self._connection = _connect_psycopg2(self._connection_string)
        else:
            self._connection = _connect_pg8000(self._connection_string)

    def _disconnect(self):
        self._connection.close()
        self._connection = None

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

    @property
    def encoding(self):
        return self._connection.encoding


class PostgresqlBackend(object):
    def __init__(self, connection_string="", table_prefix="", library="psycopg2"):
        self._connection = PostgresqlConnection(connection_string, library)
        self._library = library

        if table_prefix and not re.match(r"[a-z][_a-z]*(\.[a-z][_a-z]*)*", table_prefix):
            raise ValueError("invalid table_prefix %s" % table_prefix)
        self._table_prefix = table_prefix

        self._core_table_name = self._table_name("core")
        self._link_table_name = self._table_name("link")
        self._tag_table_name = self._table_name("tag")

        self._namespace_schemas = {}
        self._sql_builder = sql.SQLBuilder({}, sql.TypeMap(), {}, self._table_name, self._placeholder,
                                           self._placeholder, self._rewriter_property)

    def _create_tables_sql(self):
        result = []
        # Create the table for the core namespace.
        result.append(self._sql_builder.build_create_table_query("core"))
        result.append("ALTER TABLE %s ADD PRIMARY KEY (uuid)" % self._core_table_name)
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_archive_path_uniq UNIQUE (archive_path, physical_name)" %
                      (self._core_table_name, self._core_table_name))
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_product_name_uniq UNIQUE (product_type, product_name)" %
                      (self._core_table_name, self._core_table_name))

        schema = self._namespace_schema("core")
        for name in schema:
            if schema.has_index(name):
                if schema[name] == Geometry:
                    # For the geospatial footprint we need to use a special GIST index
                    result.append("CREATE INDEX idx_%s_%s ON %s USING GIST (%s)" %
                                  (self._core_table_name, name, self._core_table_name, name))
                else:
                    result.append("CREATE INDEX idx_%s_%s ON %s (%s)" %
                                  (self._core_table_name, name, self._core_table_name, name))

        # Create the tables for all non-core namespaces.
        for namespace in self._namespace_schemas:
            if namespace == "core":
                continue

            result.append(self._sql_builder.build_create_table_query(namespace))

            result.append("ALTER TABLE %s ADD COLUMN uuid UUID PRIMARY KEY" % self._table_name(namespace))
            result.append("ALTER TABLE %s ADD CONSTRAINT %s_uuid_fkey FOREIGN KEY (uuid) REFERENCES %s (uuid) ON "
                          "DELETE CASCADE" % (self._table_name(namespace), self._table_name(namespace),
                                              self._core_table_name))

            schema = self._namespace_schema(namespace)
            for name in schema:
                if schema.has_index(name):
                    if schema[name] == Geometry:
                        result.append("CREATE INDEX idx_%s_%s ON %s USING GIST (%s)" %
                                      (self._table_name(namespace), name, self._table_name(namespace), name))
                    else:
                        result.append("CREATE INDEX idx_%s_%s ON %s (%s)" %
                                      (self._table_name(namespace), name, self._table_name(namespace), name))

        # We use explicit 'id' primary keys for the links and tags tables so the entries can be managed using
        # other front-ends that may not support tuples as primary keys.

        # Create the table for links.
        result.append("CREATE TABLE %s (id SERIAL PRIMARY KEY, uuid UUID NOT NULL, source_uuid UUID NOT NULL)" %
                      self._link_table_name)
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_link_uuid_source_uuid_uniq UNIQUE (uuid, source_uuid)" %
                      (self._link_table_name, self._link_table_name))
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_uuid_fkey FOREIGN KEY (uuid) REFERENCES %s (uuid) ON "
                      "DELETE CASCADE" % (self._link_table_name, self._link_table_name, self._core_table_name))
        result.append("CREATE INDEX idx_%s_uuid ON %s (uuid)" % (self._link_table_name, self._link_table_name))
        result.append("CREATE INDEX idx_%s_source_uuid ON %s (source_uuid)" %
                      (self._link_table_name, self._link_table_name))

        # Create the table for tags.
        result.append("CREATE TABLE %s (id SERIAL PRIMARY KEY, uuid UUID NOT NULL, tag TEXT NOT NULL)" %
                      self._tag_table_name)
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_tag_uuid_tag_uniq UNIQUE (uuid, tag)" %
                      (self._tag_table_name, self._tag_table_name))
        result.append("ALTER TABLE %s ADD CONSTRAINT %s_uuid_fkey FOREIGN KEY (uuid) REFERENCES %s (uuid) ON "
                      "DELETE CASCADE" % (self._tag_table_name, self._tag_table_name, self._core_table_name))
        result.append("CREATE INDEX idx_%s_uuid ON %s (uuid)" % (self._tag_table_name, self._tag_table_name))
        result.append("CREATE INDEX idx_%s_tag ON %s (tag)" % (self._tag_table_name, self._tag_table_name))
        return result

    def _delete_product_properties(self, uuid):
        cursor = self._connection.cursor()
        try:
            cursor.execute("DELETE FROM %s WHERE source_uuid = %s" % (self._link_table_name, self._placeholder()),
                           (uuid,))
            cursor.execute("DELETE FROM %s WHERE uuid = %s" % (self._core_table_name, self._placeholder()), (uuid,))
            assert cursor.rowcount <= 1

            if cursor.rowcount != 1:
                raise Error("could not delete properties for product: %s" % (uuid,))
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

    def _execute_list(self, sql_list):
        cursor = self._connection.cursor()
        try:
            for item in sql_list:
                cursor.execute(item)
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

    def _insert_namespace_properties(self, uuid, name, properties):
        self._validate_namespace_properties(name, properties)
        assert uuid is not None and getattr(properties, "uuid", uuid) == uuid

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

        # TODO generic conversion? more overlap between sqlite.py and postgresql.py?
        parameters = [json.dumps(p) if isinstance(p, dict) else p for p in parameters]

        # Build and execute INSERT query.
        query = "INSERT INTO %s (%s) VALUES (%s)" % (self._table_name(name), ", ".join(fields),
                                                     ", ".join([self._placeholder()] * len(fields)))

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
        finally:
            cursor.close()

    def _delete_namespace_properties(self, uuid, name):
        query = "DELETE FROM %s WHERE uuid=%s" % (self._table_name(name), self._placeholder())
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, [uuid])
        finally:
            cursor.close()

    def _swallow_unique_violation(self, _error):
        # There is still a small chance due to concurrency that a link/tag already exists.
        # For those cases we swallow the exception.
        swallow = False

        if self._library == 'psycopg2':
            try:
                if _error.pgcode == PG_UNIQUE_VIOLATION:
                    swallow = True
            except AttributeError:
                pass

        elif self._library == 'pg8000':  # TODO positional - issue filed on github
            try:
                if _error.args[0]['C'] == PG_UNIQUE_VIOLATION:
                    swallow = True
            except (TypeError, IndexError, AttributeError, KeyError):
                pass

        return swallow

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
                except self._connection._backend.Error as _error:
                    if not self._swallow_unique_violation(_error):
                        raise
                finally:
                    cursor.close()

    def _namespace_schema(self, namespace):
        try:
            return self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"" % namespace)

    def _placeholder(self, name=None, arg=None):
        if name is not None:
            result = "%%(%s)s" % name
            if isinstance(arg, datetime.datetime):
                result += "::timestamp"
        else:
            result = "%s"
        return result

    def _rewriter_property(self, column_name, subscript):
        # timestamp
        if subscript == 'year':
            return "TO_CHAR(%s, 'YYYY')" % column_name
        if subscript == 'month':
            return "TO_CHAR(%s, 'MM')" % column_name
        if subscript == 'yearmonth':
            return "TO_CHAR(%s, 'YYYY-MM')" % column_name
        if subscript == 'day':
            return "TO_CHAR(%s, 'DD')" % column_name
        if subscript == 'date':
            return "TO_CHAR(%s, 'YYYY-MM-DD')" % column_name
        if subscript == 'hour':
            return "TO_CHAR(%s, 'HH24')" % column_name
        if subscript == 'minute':
            return "TO_CHAR(%s, 'MI')" % column_name
        if subscript == 'second':
            return "TO_CHAR(%s, 'SS')" % column_name
        if subscript == 'time':
            return "TO_CHAR(%s, 'HH24:MI:SS')" % column_name
        # text
        if subscript == 'length':
            return "CHAR_LENGTH(%s)" % column_name
        raise ValueError('Unsupported subscript: %s' % subscript)

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

        def is_source_of_subquery(where_expr, where_namespaces):
            joins = ''
            for namespace in where_namespaces:
                joins = "%s INNER JOIN %s USING (uuid)" % (joins, self._table_name(namespace))

            return "{core}.uuid in (SELECT {link}.source_uuid FROM {core} {joins} " \
                "INNER JOIN {link} on {link}.uuid = {core}.uuid WHERE {where})".format(
                    core=self._core_table_name, link=self._link_table_name, joins=joins,
                    where=where_expr)

        rewriter_table[Prototype("is_source_of", (Boolean,), Boolean)] = is_source_of_subquery

        rewriter_table[Prototype("is_derived_from", (UUID,), Boolean)] = \
            lambda arg0: "EXISTS (SELECT 1 FROM %s WHERE uuid = %s.uuid AND source_uuid = (%s))" % \
            (self._link_table_name, self._core_table_name, arg0)

        def is_derived_from_subquery(where_expr, where_namespaces):
            joins = ''
            for namespace in where_namespaces:
                joins = "%s INNER JOIN %s USING (uuid)" % (joins, self._table_name(namespace))

            return "{core}.uuid in (SELECT {link}.uuid FROM {core} {joins} " \
                "INNER JOIN {link} on {link}.source_uuid = {core}.uuid WHERE {where})".format(
                    core=self._core_table_name, link=self._link_table_name, joins=joins,
                    where=where_expr)

        rewriter_table[Prototype("is_derived_from", (Boolean,), Boolean)] = is_derived_from_subquery

        rewriter_table[Prototype("has_tag", (Text,), Boolean)] = \
            lambda arg0: "EXISTS (SELECT 1 FROM %s WHERE uuid = %s.uuid AND tag = (%s))" % \
            (self._tag_table_name, self._core_table_name, arg0)

        rewriter_table[Prototype("now", (), Timestamp)] = \
            sql.as_is("now() AT TIME ZONE 'UTC'")

        def is_defined_rewriter(arg):
            namespace_name = arg.split('.')
            if len(namespace_name) == 1:
                return 'EXISTS (SELECT 1 FROM %s WHERE uuid = %s.uuid)' % \
                    (self._table_name(arg), self._core_table_name)
            else:
                return "(%s) IS NOT NULL" % arg

        rewriter_table[Prototype("is_defined", (Long,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Integer,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Real,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Boolean,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Text,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Namespace,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Timestamp,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (UUID,), Boolean)] = is_defined_rewriter
        rewriter_table[Prototype("is_defined", (Geometry,), Boolean)] = is_defined_rewriter

        return rewriter_table

    def _source_products(self, uuid):
        query = "SELECT source_uuid FROM %s WHERE uuid = %s" % (self._link_table_name, self._placeholder())
        parameters = (uuid,)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            return [row[0] for row in cursor]
        finally:
            cursor.close()

    def _table_name(self, name):
        return name if not self._table_prefix else self._table_prefix + name

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
                except self._connection._backend.Error as _error:
                    self._swallow_unique_violation(_error)
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

    def _type_map(self):
        type_map = sql.TypeMap()
        type_map[Long] = "BIGINT"
        type_map[Integer] = "INTEGER"
        type_map[Real] = "DOUBLE PRECISION"
        type_map[Boolean] = "BOOLEAN"
        type_map[Text] = "TEXT COLLATE \"C\""
        type_map[JSON] = "JSONB"
        type_map[Timestamp] = "TIMESTAMP"
        type_map[UUID] = "UUID"
        type_map[Geometry] = "GEOGRAPHY"

        return type_map

    def _unlink(self, uuid, source_uuids=None):
        if source_uuids is None:
            query = "DELETE FROM %s WHERE uuid = %s" % (self._link_table_name, self._placeholder())
            parameters = (uuid,)
        else:
            for source_uuid in source_uuids:
                query = "DELETE FROM %s WHERE uuid = %s AND source_uuid = %s" % \
                    (self._link_table_name, self._placeholder(), self._placeholder())
                parameters = (uuid, source_uuid)
                cursor = self._connection.cursor()
                try:
                    cursor.execute(query, parameters)
                finally:
                    cursor.close()

    def _untag(self, uuid, tags=None):
        if tags is None:
            query = "DELETE FROM %s WHERE uuid = %s" % (self._tag_table_name, self._placeholder())
            parameters = (uuid,)
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, parameters)
            finally:
                cursor.close()
        else:
            for tag in tags:
                query = "DELETE FROM %s WHERE uuid = %s AND tag = %s" % (self._tag_table_name,
                                                                         self._placeholder(), self._placeholder())
                parameters = (uuid, tag)
                cursor = self._connection.cursor()
                try:
                    cursor.execute(query, parameters)
                finally:
                    cursor.close()

    def _update_namespace_properties(self, uuid, name, properties):
        self._validate_namespace_properties(name, properties, partial=True)
        assert uuid is not None and getattr(properties, "uuid", uuid) == uuid

        # Split the properties into a list of (database) field names and a list of values. This assumes the database
        # field that corresponds to a given property has the same name. If the backend uses different field names, the
        # required translation can be performed here. Values can also be translated if necessary.
        properties_dict = vars(properties)
        fields, parameters = dictkeys(properties_dict), dictvalues(properties_dict)
        if not fields:
            return  # nothing to do

        # Remove the uuid field if present. This field needs to be included in the WHERE clause of the UPDATE query,
        # not in the SET clause.
        try:
            uuid_index = fields.index("uuid")
        except ValueError:
            pass
        else:
            del fields[uuid_index]
            del parameters[uuid_index]

        # Append the uuid (value) at the end of the list of parameters (will be used in the WHERE clause).
        parameters.append(uuid)

        parameters = [json.dumps(p) if isinstance(p, dict) else p for p in parameters]

        # Build and execute UPDATE query.
        set_clause = ", ".join(["%s = %s" % (field, self._placeholder()) for field in fields])
        query = "UPDATE %s SET %s WHERE uuid = %s" % (self._table_name(name), set_clause, self._placeholder())

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            assert cursor.rowcount <= 1

            if cursor.rowcount != 1:
                raise Error("could not update properties for namespace: %s for product: %s" % (name, uuid))
        finally:
            cursor.close()

    def _unpack_namespace_properties(self, namespace, description, values):
        unpacked_properties = Struct()
        schema = self._namespace_schema(namespace)
        for identifier, value in zip(description, values):
            # We may get unicode from the (psycopg2) connection
            # if, possibly by a third party, the UNICODE adapter is loaded.
            # Muninn assumes strs
            if is_python2_unicode(value):
                value = value.encode(self._connection.encoding)

            if value is not None or not schema.is_optional(identifier):
                unpacked_properties[identifier] = value

        return unpacked_properties

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
                assert ns_description[0] == "uuid"
                if values[start] is None:
                    # Skip the entire namespace.
                    start = end
                    continue

                # Skip the uuid field.
                start += 1
                ns_description = ns_description[1:]

            unpacked_ns_properties = self._unpack_namespace_properties(ns_name, ns_description, values[start:end])
            self._validate_namespace_properties(ns_name, unpacked_ns_properties, partial=True)
            unpacked_properties[ns_name] = unpacked_ns_properties
            start = end

        return unpacked_properties

    def _validate_namespace_properties(self, namespace, properties, partial=False):
        self._namespace_schema(namespace).validate(properties, partial)

    @translate_errors
    def count(self, where="", parameters={}):
        query, query_parameters = self._sql_builder.build_count_query(where, parameters)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                assert cursor.rowcount == 1
                return cursor.fetchone()[0]
            finally:
                cursor.close()

    @translate_errors
    def delete_product_properties(self, uuid):
        with self._connection:
            self._delete_product_properties(uuid)

    @translate_errors
    def derived_products(self, uuid):
        with self._connection:
            return self._derived_products(uuid)

    @translate_errors
    def destroy(self):
        self._drop_tables()

    @translate_errors
    def disconnect(self):
        """Drop the connection to the database in order to free up resources. The connection will be re-established
        automatically when required."""
        self._connection.close()

    @translate_errors
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

    @translate_errors
    def find_products_without_available_source(self, product_type=None, grace_period=datetime.timedelta()):
        """Return the core properties of all products that are linked to one or more source products, all of which are
           unavailable. A product is unavailable if there is no data associated with it, only properties. Products that
           have links to external source products will not be selected by this function, because it cannot be
           determined whether or not these products are available.

           Keyword arguments:
           product_type --  Only consider products of the specified product type.

        """
        with self._connection:
            return self._find_products_without_available_source(product_type, grace_period)

    @translate_errors
    def find_products_without_source(self, product_type=None, grace_period=datetime.timedelta(),
                                     archived_only=False):
        """Return the core properties of all products that are not linked to any source products.

           Keyword arguments:
           product_type --  Only consider products of the specified product type.

        """
        with self._connection:
            return self._find_products_without_source(product_type, grace_period, archived_only)

    def initialize(self, namespace_schemas):
        self._namespace_schemas = namespace_schemas
        self._sql_builder = sql.SQLBuilder(self._namespace_schemas, self._type_map(), self._rewriter_table(),
                                           self._table_name, self._placeholder, self._placeholder,
                                           self._rewriter_property)

    @translate_errors
    def insert_product_properties(self, properties):
        with self._connection:
            self._insert_namespace_properties(properties.core.uuid, "core", properties.core)
            for ns_name, ns_properties in vars(properties).items():
                if ns_name == "core" or ns_properties is None:
                    continue
                self._insert_namespace_properties(properties.core.uuid, ns_name, ns_properties)

    @translate_errors
    def link(self, uuid, source_uuids):
        self._link(uuid, source_uuids)

    @translate_errors
    def prepare(self, dry_run=False):
        sqls = self._create_tables_sql()
        if not dry_run:
            with self._connection:
                self._execute_list(sqls)
        return sqls

    @translate_errors
    def search(self, where="", order_by=[], limit=None, parameters={}, namespaces=[], property_names=[]):
        query, query_parameters, query_description = \
            self._sql_builder.build_search_query(where, order_by, limit, parameters, namespaces, property_names)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                return [self._unpack_product_properties(query_description, row) for row in cursor]
            finally:
                cursor.close()

    @translate_errors
    def server_time_utc(self):
        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute("SELECT timezone(%s, now())", ("UTC",))
                assert cursor.rowcount == 1
                return cursor.fetchone()[0]
            finally:
                cursor.close()

    @translate_errors
    def source_products(self, uuid):
        with self._connection:
            return self._source_products(uuid)

    @translate_errors
    def summary(self, where="", parameters=None, aggregates=None, group_by=None, group_by_tag=False,
                having=None, order_by=None):
        query, query_parameters, query_description = self._sql_builder.build_summary_query(
            where, parameters, aggregates, group_by, group_by_tag, having, order_by)

        with self._connection:
            cursor = self._connection.cursor()
            try:
                cursor.execute(query, query_parameters)
                return [row for row in cursor], query_description
            finally:
                cursor.close()

    @translate_errors
    def tag(self, uuid, tags):
        self._tag(uuid, tags)

    @translate_errors
    def tags(self, uuid):
        with self._connection:
            return self._tags(uuid)

    @translate_errors
    def unlink(self, uuid, source_uuids=None):
        with self._connection:
            self._unlink(uuid, source_uuids)

    @translate_errors
    def untag(self, uuid, tags=None):
        with self._connection:
            self._untag(uuid, tags)

    @translate_errors
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
                    if ns_properties is not None:
                        self._insert_namespace_properties(uuid, ns_name, ns_properties)
                elif ns_properties is None:
                    self._delete_namespace_properties(uuid, ns_name)
                else:
                    self._update_namespace_properties(uuid, ns_name, ns_properties)
