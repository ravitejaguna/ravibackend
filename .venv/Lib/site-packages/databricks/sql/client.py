"""DB-API implementation backed by Databricks.

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

import base64
import datetime
import re
from decimal import Decimal
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED, create_default_context


from databricks.sql import common, USER_AGENT_NAME, __version__
from databricks.sql.exc import *
from databricks.sql.TCLIService import TCLIService, ttypes
from builtins import range
from future.utils import iteritems
import logging
import sys
import thrift.transport.THttpClient
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

_logger = logging.getLogger(__name__)

_TIMESTAMP_PATTERN = re.compile(r'(\d+-\d+-\d+ \d+:\d+:\d+(\.\d{,6})?)')

ssl_cert_parameter_map = {
    "none": CERT_NONE,
    "optional": CERT_OPTIONAL,
    "required": CERT_REQUIRED,
}

def _parse_timestamp(value):
    if value:
        match = _TIMESTAMP_PATTERN.match(value)
        if match:
            if match.group(2):
                format = '%Y-%m-%d %H:%M:%S.%f'
                # use the pattern to truncate the value
                value = match.group()
            else:
                format = '%Y-%m-%d %H:%M:%S'
            value = datetime.datetime.strptime(value, format)
        else:
            raise Exception(
                'Cannot convert "{}" into a datetime'.format(value))
    else:
        value = None
    return value


def _parse_date(value):
    if value:
        format = '%Y-%m-%d'
        value = datetime.datetime.strptime(value, format).date()
    else:
        value = None
    return value


TYPES_CONVERTER = {"decimal": Decimal,
                   "timestamp": _parse_timestamp,
                   "date": _parse_date}


class _HiveParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(
            item
                .replace('\\', '\\\\')
                .replace("'", "\\'")
                .replace('\r', '\\r')
                .replace('\n', '\\n')
                .replace('\t', '\\t')
        )


_escaper = _HiveParamEscaper()


class Connection(object):
    """Wraps a Thrift session"""

    def __init__(
            self,
            server_hostname,
            http_path,
            access_token,
            **kwargs
    ):
        """Connect to a Databricks SQL endpoint or a Databricks cluster.

        :param server_hostname: Databricks instance host name.
        :param http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
               or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
        :param access_token: Http Bearer access token, e.g. Databricks Personal Access Token.
        """

        # Internal arguments in **kwargs:
        # _user_agent_entry
        #   Tag to add to User-Agent header. For use by partners.
        # _username, _password
        #   Username and password Basic authentication (no official support)
        # _tls_no_verify
        #   Set to True (Boolean) to completely disable SSL verification.
        # _tls_verify_hostname
        #   Set to False (Boolean) to disable SSL hostname verification, but check certificate.
        # _tls_trusted_ca_file
        #   Set to the path of the file containing trusted CA certificates for server certificate
        #   verification. If not provide, uses system truststore.
        # _tls_client_cert_file, _tls_client_cert_key_file, _tls_client_cert_key_password
        #   Set client SSL certificate.
        #   See https://docs.python.org/3/library/ssl.html#ssl.SSLContext.load_cert_chain
        # _connection_uri
        #   Overrides server_hostname and http_path.

        port = 443
        if kwargs.get("_connection_uri"):
            uri = kwargs.get("_connection_uri")
        elif server_hostname and http_path:
            uri = "https://{host}:{port}/{path}".format(
                host=server_hostname, port=port, path=http_path.lstrip("/"))
        else:
            raise ValueError("No valid connection settings.")

        # Configure tls context
        ssl_context = create_default_context(cafile=kwargs.get("_tls_trusted_ca_file"))
        if kwargs.get("_tls_no_verify") is True:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE
        elif kwargs.get("_tls_verify_hostname") is False:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.check_hostname = True
            ssl_context.verify_mode = CERT_REQUIRED

        tls_client_cert_file = kwargs.get("_tls_client_cert_file")
        tls_client_cert_key_file = kwargs.get("__tls_client_cert_key_file")
        tls_client_cert_key_password = kwargs.get("_tls_client_cert_key_password")
        if tls_client_cert_file:
            ssl_context.load_cert_chain(
                certfile=tls_client_cert_file,
                keyfile=tls_client_cert_key_file,
                password=tls_client_cert_key_password
            )

        self._transport = thrift.transport.THttpClient.THttpClient(
            uri_or_host=uri,
            ssl_context=ssl_context,
        )

        if kwargs.get("_username") and kwargs.get("_password"):
            auth_credentials = "{username}:{password}".format(
                username=kwargs.get("_username"), password=kwargs.get("_password")
            ).encode("UTF-8")
            auth_credentials_base64 = base64.standard_b64encode(auth_credentials).decode(
                "UTF-8"
            )
            authorization_header = "Basic {}".format(auth_credentials_base64)
        elif access_token:
            authorization_header = "Bearer {}".format(access_token)
        else:
            raise ValueError("No valid authentication settings.")

        if not kwargs.get("_user_agent_entry"):
            useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)
        else:
            useragent_header = "{}/{} ({})".format(
                USER_AGENT_NAME, __version__, kwargs.get("_user_agent_entry"))

        self._transport.setCustomHeaders({
            "Authorization" : authorization_header,
            "User-Agent" : useragent_header
        })
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = TCLIService.Client(protocol)
        # oldest version that still contains features we care about
        # "V6 uses binary type for binary payload (was string) and uses columnar result set"
        protocol_version = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6

        try:
            self._transport.open()
            open_session_req = ttypes.TOpenSessionReq(
                client_protocol=protocol_version
            )
            response = self._client.OpenSession(open_session_req)
            _check_status(response)
            assert response.sessionHandle is not None, "Expected a session from OpenSession"
            self._sessionHandle = response.sessionHandle
            assert response.serverProtocolVersion == protocol_version, \
                "Unable to handle protocol version {}".format(response.serverProtocolVersion)
        except:
            self._transport.close()
            raise

    def __enter__(self):
        """Transport should already be opened by __init__"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close"""
        self.close()

    def close(self):
        """Close the underlying session and Thrift transport"""
        req = ttypes.TCloseSessionReq(sessionHandle=self._sessionHandle)
        response = self._client.CloseSession(req)
        self._transport.close()
        _check_status(response)

    def commit(self):
        """No-op because Databricks does not support transactions"""
        pass

    def cursor(self, *args, **kwargs):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self, *args, **kwargs)

    def rollback(self):
        raise NotSupportedError("Databricks does not have transactions")


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, connection, arraysize=10000):
        self._operationHandle = None
        super(Cursor, self).__init__()
        self._arraysize = arraysize
        self._connection = connection

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._description = None
        if self._operationHandle is not None:
            request = ttypes.TCloseOperationReq(self._operationHandle)
            try:
                response = self._connection._client.CloseOperation(request)
                _check_status(response)
            finally:
                self._operationHandle = None

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        """Array size cannot be None, and should be an integer"""
        default_arraysize = 10000
        try:
            self._arraysize = int(value) or default_arraysize
        except TypeError:
            self._arraysize = default_arraysize

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._operationHandle is None or not self._operationHandle.hasResultSet:
            return None
        if self._description is None:
            req = ttypes.TGetResultSetMetadataReq(self._operationHandle)
            response = self._connection._client.GetResultSetMetadata(req)
            _check_status(response)
            columns = response.schema.columns
            self._description = []
            for col in columns:
                primary_type_entry = col.typeDesc.types[0]
                if primary_type_entry.primitiveEntry is None:
                    # All fancy stuff maps to string
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[ttypes.TTypeId.STRING_TYPE]
                else:
                    type_id = primary_type_entry.primitiveEntry.type
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
                # Stripping "_TYPE" and converting to lowercase makes TTypeIds consistent with
                # Spark types.
                if type_code.endswith("_TYPE"):
                    type_code = type_code[:-5]
                type_code = type_code.lower()

                self._description.append((
                    col.columnName.decode('utf-8') if sys.version_info[0] == 2 else col.columnName,
                    type_code.decode('utf-8') if sys.version_info[0] == 2 else type_code,
                    None, None, None, None, True
                ))
        return self._description

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the operation handle"""
        self._reset_state()

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        _logger.info('%s', sql)

        req = ttypes.TExecuteStatementReq(self._connection._sessionHandle,
                                          sql, runAsync=True)
        _logger.debug(req)
        response = self._connection._client.ExecuteStatement(req)
        _check_status(response)
        self._operationHandle = response.operationHandle

        self.__wait_for_query_completion()

    def cancel(self):
        req = ttypes.TCancelOperationReq(
            operationHandle=self._operationHandle,
        )
        response = self._connection._client.CancelOperation(req)
        _check_status(response)

    def _fetch_more(self):
        """Send another TFetchResultsReq and update state"""
        assert(self._state == self._STATE_RUNNING), "Should be running when in _fetch_more"
        assert(self._operationHandle is not None), "Should have an op handle in _fetch_more"
        if not self._operationHandle.hasResultSet:
            raise ProgrammingError("No result set")
        req = ttypes.TFetchResultsReq(
            operationHandle=self._operationHandle,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
            maxRows=self.arraysize,
        )
        response = self._connection._client.FetchResults(req)
        _check_status(response)
        schema = self.description
        assert not response.results.rows, 'expected data in columnar format'
        columns = [_unwrap_column(col, col_schema[1]) for col, col_schema in
                   zip(response.results.columns, schema)]
        new_data = list(zip(*columns))
        self._data += new_data
        # response.hasMoreRows seems to always be False, so we instead check the number of rows
        # https://github.com/apache/hive/blob/release-1.2.1/service/src/java/org/apache/hive/service/cli/thrift/ThriftCLIService.java#L678
        # if not response.hasMoreRows:
        if not new_data:
            self._state = self._STATE_FINISHED

    def __wait_for_query_completion(self):
        state = ttypes.TOperationState.INITIALIZED_STATE
        while state in [ttypes.TOperationState.INITIALIZED_STATE, ttypes.TOperationState.PENDING_STATE, ttypes.TOperationState.RUNNING_STATE]:
            resp = self.__poll()
            state = resp.operationState

        if state in [ttypes.TOperationState.ERROR_STATE,
                     ttypes.TOperationState.UKNOWN_STATE,
                     ttypes.TOperationState.CANCELED_STATE,
                     ttypes.TOperationState.TIMEDOUT_STATE]:
            raise OperationalError("Query execution failed.\nState: {}; Error code: {}; SQLSTATE: {}\nError message: {}\n"
                                   .format(ttypes.TOperationState._VALUES_TO_NAMES[state],
                                           resp.errorCode,
                                           resp.errorMessage,
                                           resp.sqlState))

    def __poll(self, get_progress_update=True):
        """Poll for and return the raw status data provided by the Hive Thrift REST API.
        :returns: ``ttypes.TGetOperationStatusResp``
        :raises: ``ProgrammingError`` when no query has been started
        .. note::
            This is not a part of DB-API.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")

        req = ttypes.TGetOperationStatusReq(
            operationHandle=self._operationHandle,
            getProgressUpdate=get_progress_update,
        )
        response = self._connection._client.GetOperationStatus(req)
        _check_status(response)

        return response

#
# Private utilities
#


def _unwrap_column(col, type_=None):
    """Return a list of raw values from a TColumn instance."""
    for attr, wrapper in iteritems(col.__dict__):
        if wrapper is not None:
            result = wrapper.values
            nulls = wrapper.nulls  # bit set describing what's null
            assert isinstance(nulls, bytes)
            for i, char in enumerate(nulls):
                byte = ord(char) if sys.version_info[0] == 2 else char
                for b in range(8):
                    if byte & (1 << b):
                        result[i * 8 + b] = None
            converter = TYPES_CONVERTER.get(type_, None)
            if converter and type_:
                result = [converter(row) if row else row for row in result]
            return result
    raise DataError("Got empty column value {}".format(col))  # pragma: no cover


def _check_status(response):
    """Raise an OperationalError if the status is not success"""
    _logger.debug(response)
    if response.status.statusCode != ttypes.TStatusCode.SUCCESS_STATUS:
        raise OperationalError(response)
