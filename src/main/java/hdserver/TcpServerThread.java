package hdserver;

import org.h2.api.ErrorCode;
import org.h2.command.Command;
import org.h2.engine.*;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.jdbc.JdbcException;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.util.*;
import org.h2.value.*;
import java.io.*;
import java.net.Socket;
import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;

public class TcpServerThread implements Runnable {
    private DuckDBSessionLocal duckdbSession;
    private DuckDBCommand _commit;
    private final SmallMap _cache =
            new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final SmallLRUCache<Long, CachedInputStream> _lobs =
            SmallLRUCache.newInstance(Math.max(
                    SysProperties.SERVER_CACHED_OBJECTS,
                    SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));


    private void setDuckDBParameters(ArrayList<Value> values, DuckDBCommand duckDBCommand)   {
        for (int i = 0; i < values.size(); i++) {
            duckDBCommand.setParameterValue(i+1, values.get(i));
        }
    }

    public void checkAndCancelTimeoutTask() {
        if (duckdbSession != null && !duckdbSession.isClosed())
            duckdbSession.cancelTimeoutStatments();
    }

    private boolean stop;
    private Thread thread;
    private final TcpServer server;
    private final int threadId;
    private int clientVersion;
    private String sessionId;

    protected final DuckDBTransfer transfer;

    TcpServerThread(Socket socket, TcpServer server, int id) {
        this.server = server;
        this.threadId = id;
        transfer = new DuckDBTransfer(socket);
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

    @Override
    public void run() {
        try {
            transfer.init();
            trace("Connect");

            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                Socket socket = transfer.getSocket();
                if (socket == null) {
                    // the transfer is already closed, prevent NPE in TcpServer#allow(Socket)
                    return;
                }
                if (!server.allow(transfer.getSocket())) {
                    throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }

                int minClientVersion = transfer.readInt();
                if (minClientVersion < 6) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            Integer.toString(minClientVersion), "" + Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED);
                }
                int maxClientVersion = transfer.readInt();
                if (maxClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            Integer.toString(maxClientVersion), "" + Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED);
                } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            Integer.toString(minClientVersion), "" + Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED);
                }
                if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED;
                } else {
                    clientVersion = maxClientVersion;
                }
                transfer.setVersion(clientVersion);
                String db = transfer.readString();
                String originalURL = transfer.readString();
                if (db == null && originalURL == null) {
                    String targetSessionId = transfer.readString();
                    int command = transfer.readInt();
                    stop = true;
                    if (command == SessionRemote.SESSION_CANCEL_STATEMENT) {
                        // cancel a running statement
                        int statementId = transfer.readInt();
                        server.cancelStatement(targetSessionId, statementId);
                    } else if (command == SessionRemote.SESSION_CHECK_KEY) {
                        // check if this is the correct server
                        db = server.checkKeyAndGetDatabaseName(targetSessionId);
                        if (!targetSessionId.equals(db)) {
                            transfer.writeInt(SessionRemote.STATUS_OK);
                        } else {
                            transfer.writeInt(SessionRemote.STATUS_ERROR);
                        }
                    }
                }
                String baseDir = server.getBaseDir();
                if (baseDir == null) {
                    baseDir = SysProperties.getBaseDir();
                }
                db = server.checkKeyAndGetDatabaseName(db);
                ConnectionInfo ci = new ConnectionInfo(db);
                ci.setOriginalURL(originalURL);
                ci.setUserName(transfer.readString());
                ci.setUserPasswordHash(transfer.readBytes());
                ci.setFilePasswordHash(transfer.readBytes());
                int len = transfer.readInt();
                for (int i = 0; i < len; i++) {
                    ci.setProperty(transfer.readString(), transfer.readString());
                }
                // override client's requested properties with server settings
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if (server.getIfExists()) {
                    ci.setProperty("FORBID_CREATION", "TRUE");
                }
                transfer.writeInt(SessionRemote.STATUS_OK);
                transfer.writeInt(clientVersion);
                transfer.flush();
                if (ci.getFilePasswordHash() != null) {
                    ci.setFileEncryptionKey(transfer.readBytes());
                }
                ci.setNetworkConnectionInfo(new NetworkConnectionInfo(
                        NetUtils.ipToShortForm(new StringBuilder(server.getSSL() ? "ssl://" : "tcp://"),
                                socket.getLocalAddress().getAddress(), true) //
                                .append(':').append(socket.getLocalPort()).toString(), //
                        socket.getInetAddress().getAddress(), socket.getPort(),
                        new StringBuilder().append('P').append(clientVersion).toString()));
                if (clientVersion < Constants.TCP_PROTOCOL_VERSION_20) {
                    // For DatabaseMetaData
                    ci.setProperty("OLD_INFORMATION_SCHEMA", "TRUE");
                    // For H2 Console
                    ci.setProperty("NON_KEYWORDS", "VALUE");
                }

                String duckDBfile = db;
                duckdbSession = DuckDBSessionLocal.createSession(duckDBfile);

                server.addConnection(threadId, originalURL, ci.getUserName());
                trace("Connected");
            } catch (OutOfMemoryError e) {
                // catch this separately otherwise such errors will never hit the console
                server.traceError(e);
                sendError(e, true);
                stop = true;
            } catch (Throwable e) {
                sendError(e,true);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    sendError(e, true);
                    if (e.getMessage() != null) {
                        Log.warning("send error:" +
                                e.getClass().toString() +
                                " msg:" + e.getMessage());
                        e.printStackTrace();
                    } else {
                        Log.info("send empty error:" + e.getClass().toString());
                    }
                }
            }
            trace("Disconnect");
        } catch (Throwable e) {
            server.traceError(e);
        } finally {
            close();
        }
    }

    private void _closeSession() {
        if (duckdbSession != null) {
            RuntimeException closeError = null;
            try {
                duckdbSession.close();
                server.removeConnection(threadId);
            } catch (RuntimeException e) {
                closeError = e;
                server.traceError(e);
            } catch (Exception e) {
                server.traceError(e);
            } finally {
                duckdbSession = null;
            }
            if (closeError != null) {
                throw closeError;
            }
        }
    }

    /**
     * Close a connection.
     */
    void close() {
        try {
            stop = true;

            _closeSession();
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            transfer.close();
            trace("Close");
            server.remove(this);
        }
    }

    private void sendError(Throwable t, boolean withStatus) {
        try {
            SQLException e = DbException.convert(t).getSQLException();
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            String message;
            String sql;
            if (e instanceof JdbcException) {
                JdbcException j = (JdbcException) e;
                message = j.getOriginalMessage();
                sql = j.getSQL();
            } else {
                message = e.getMessage();
                sql = null;
            }
            if (withStatus) {
                transfer.writeInt(SessionRemote.STATUS_ERROR);
            }
            transfer.
                    writeString(e.getSQLState()).writeString(message).
                    writeString(sql).writeInt(e.getErrorCode()).writeString(trace).flush();
        } catch (Exception e2) {
            if (!transfer.isClosed()) {
                server.traceError(e2);
            }
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private ArrayList<Value> readTransferValues(DuckDBTransfer transfer) throws IOException {
        ArrayList<Value> res = new ArrayList<Value>();
        int len = transfer.readInt();
        for (int i = 0; i < len; i++) {
            res.add(transfer.readValue(null));
        }
        return res;
    }

    private void setParameters_x(ArrayList<Value> values,Command command)  {
        ArrayList<? extends ParameterInterface> params = command.getParameters();
        for (int i = 0; i < values.size(); i++) {
            Parameter p = (Parameter) params.get(i);
            p.setValue(values.get(i));
        }

    }

    private void process() throws IOException {
        int operation = transfer.readInt();
        switch (operation) {
            case SessionRemote.SESSION_PREPARE:
            case SessionRemote.SESSION_PREPARE_READ_PARAMS2: {
                int id = transfer.readInt();
                String sql = transfer.readString();

                Log.info("Prepare SQL:" + sql);

                long tBegin = System.currentTimeMillis();
                DuckDBCommand _command = DuckDBCommand.newInstance(duckdbSession, sql);
                long tCost = System.currentTimeMillis()-tBegin;

                Log.info("sql prepare: (ms):" + tCost);

                _cache.addObject(id, _command);
                boolean _isQuery = _command.isQuery();

                boolean _readonly = false;


                transfer.writeInt(_getState()).writeBoolean(_isQuery).
                        writeBoolean(_readonly);

                if (operation != SessionRemote.SESSION_PREPARE) {
                    transfer.writeInt(_command.getCommandType()); //return unknow command type
                }

                ParameterMetaData pMeta = _command.getParameterMetaData();

                try {
                    transfer.writeInt(pMeta.getParameterCount());
                    for(int i = 1; i <= pMeta.getParameterCount(); i++){
                        int valueType = DataType.convertSQLTypeToValueType(pMeta.getParameterType(i));
                        TypeInfo info = TypeInfo.getTypeInfo(valueType);
                        int isNullable = pMeta.isNullable(i);
                        transfer.writeTypeInfo(info).writeInt(isNullable);
                    }
                } catch (Exception e) {
                    throw DbException.convert(e);
                }

                transfer.flush();
                break;
            }
            case SessionRemote.SESSION_CLOSE: {
                stop = true;

                _closeSession();
                transfer.writeInt(SessionRemote.STATUS_OK).flush();
                close();
                break;
            }
            case SessionRemote.COMMAND_COMMIT: {
                if (_commit == null) {
                    _commit = DuckDBCommand.newInstance(duckdbSession, "COMMIT");
                }
                _commit.executeUpdate();

                transfer.writeInt(_getState()).flush();
                break;
            }
            case SessionRemote.COMMAND_GET_META_DATA: {
                throw DbException.getUnsupportedException("COMMAND:COMMAND_GET_META_DATA unsupported");
            }
            case SessionRemote.COMMAND_EXECUTE_QUERY: {
                int id = transfer.readInt();
                int objectId = transfer.readInt();
                long maxRows = transfer.readRowCount();
                int fetchSize = transfer.readInt();

                ArrayList<Value> values = readTransferValues(transfer);

                int state = _getState();

                DuckDBCommand _command = (DuckDBCommand) _cache.getObject(id, false);
                if (_command.isClosed())
                    _command.tryReset();
                long tBegin = System.currentTimeMillis();
                setDuckDBParameters(values,_command);
                SimpleResult _result;
                synchronized (duckdbSession) {
                    _result = _command.executeQuery();
                }

                long tCost = System.currentTimeMillis()-tBegin;
                Log.warning("sql query run: (ms):" + tCost );

                ResultSetMetaData resultSetMetaData= null;
                _cache.addObject(objectId, _result);

                int _columnCount = _result.getVisibleColumnCount();
                transfer.writeInt(state).writeInt(_columnCount);
                long _rowCount = _result.isLazy() ? -1L : _result.getRowCount();
                transfer.writeRowCount(_rowCount);
                for (int i = 0; i < _columnCount; i++) {
                    DuckDBResultColumn.writeColumn(transfer, _result, i);
                }
                sendRows(_result, _rowCount >= 0L ? Math.min(_rowCount, fetchSize) : fetchSize);

                transfer.flush();

                break;
            }
            case SessionRemote.COMMAND_EXECUTE_UPDATE: {
                int id = transfer.readInt();
                ArrayList<Value> values = readTransferValues(transfer);
                int mode = transfer.readInt();

                boolean writeGeneratedKeys = true;
                Object generatedKeysRequest;

                boolean isNoneGen = false;
                switch (mode) {
                    case GeneratedKeysMode.NONE:
                        generatedKeysRequest = false;
                        writeGeneratedKeys = false;
                        break;
                    case GeneratedKeysMode.AUTO:
                        generatedKeysRequest = true;
                        break;
                    case GeneratedKeysMode.COLUMN_NUMBERS: {
                        int len = transfer.readInt();
                        int[] keys = new int[len];
                        for (int i = 0; i < len; i++) {
                            keys[i] = transfer.readInt();
                        }
                        generatedKeysRequest = keys;
                        break;
                    }
                    case GeneratedKeysMode.COLUMN_NAMES: {
                        int len = transfer.readInt();
                        String[] keys = new String[len];
                        for (int i = 0; i < len; i++) {
                            keys[i] = transfer.readString();
                        }
                        generatedKeysRequest = keys;
                        break;
                    }
                    default:
                        throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                                "Unsupported generated keys' mode " + mode);
                }
                if (writeGeneratedKeys != false)
                    throw new IOException("GeneratedKeysMode Error");

                int status;
                if (duckdbSession.isClosed()) {
                    status = SessionRemote.STATUS_CLOSED;
                    stop = true;
                } else {
                    status = _getState();
                }

                DuckDBCommand _command = (DuckDBCommand) _cache.getObject(id, false);

                if( _command.isClosed())
                    _command.tryReset();
                setDuckDBParameters(values, _command);
                int  _updateCount;

                synchronized (duckdbSession) {
                        _updateCount = _command.executeUpdate();
                }

                transfer.writeInt(status);
                transfer.writeRowCount(_updateCount);
                transfer.writeBoolean(duckdbSession.getAutoCommit());

                transfer.flush();
                break;
            }
            case SessionRemote.COMMAND_CLOSE: {
                int id = transfer.readInt();

                DuckDBCommand _command = (DuckDBCommand) _cache.getObject(id, true);
                if (_command != null) {
                    if (!_command.isClosed())
                        _command.close();
                    _cache.freeObject(id);
                }

                break;
            }
            case SessionRemote.RESULT_FETCH_ROWS: {
                int id = transfer.readInt();
                int count = transfer.readInt();
                ResultInterface _result = (ResultInterface) _cache.getObject(id, false);
                transfer.writeInt(SessionRemote.STATUS_OK);
                sendRows(_result, count);
                transfer.flush();

                break;
            }
            case SessionRemote.RESULT_RESET: {
                int id = transfer.readInt();

                ResultInterface _result = (ResultInterface) _cache.getObject(id, false);
                _result.reset();
                break;
            }
            case SessionRemote.RESULT_CLOSE: {
                int id = transfer.readInt();

                ResultInterface  _result = (ResultInterface)_cache.getObject(id, true);
                if (_result != null){
                    _result.close();
                    _cache.freeObject(id);
                }

                break;
            }
            case SessionRemote.CHANGE_ID: {
                int oldId = transfer.readInt();
                int newId = transfer.readInt();

                Object _obj = _cache.getObject(oldId, false);
                _cache.freeObject(oldId);
                _cache.addObject(newId, _obj);
                break;
            }
            case SessionRemote.SESSION_SET_ID: {
                sessionId = transfer.readString();
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_20) {
                    String sid = transfer.readString();
                    duckdbSession.setTimeZone(TimeZoneProvider.ofId(sid));
                }

                transfer.writeInt(SessionRemote.STATUS_OK)
                        .writeBoolean(duckdbSession.getAutoCommit())
                        .flush();
                break;
            }
            case SessionRemote.SESSION_SET_AUTOCOMMIT: {
                boolean autoCommit = transfer.readBoolean();
                duckdbSession.setAutoCommit(autoCommit);

                transfer.writeInt(SessionRemote.STATUS_OK).flush();
                break;
            }
            case SessionRemote.SESSION_HAS_PENDING_TRANSACTION: {
                //todo
                transfer.writeInt(SessionRemote.STATUS_OK).
                        writeInt(duckdbSession.hasPendingTransaction() ? 1 : 0).flush();

                break;
            }
            case SessionRemote.LOB_READ: {
                System.out.println("LOB_READ");
                throw DbException.getUnsupportedException("COMMAND:LOB_READ unsupported");

            }
            case SessionRemote.GET_JDBC_META: {
                int code = transfer.readInt();
                Log.info("GET_JDBC_META:"+code);
                int length = transfer.readInt();
                Value[] args = new Value[length];
                for (int i = 0; i < length; i++) {
                    args[i] = transfer.readValue(null);
                }

                SimpleResult _result;
                synchronized (duckdbSession) {
                    _result = duckdbSession.getJdbcMetaData(code,args);
                }
                int _columnCount = _result.getVisibleColumnCount();
                int _state = _getState();
                transfer.writeInt(_state).writeInt(_columnCount);
                long _rowCount = _result.getRowCount();
                transfer.writeRowCount(_rowCount);
                for (int i = 0; i < _columnCount; i++) {
                    DuckDBResultColumn.writeColumn(transfer, _result, i);
                }
                sendRows(_result, _rowCount);

                transfer.flush();
                Log.info("GET_JDBC_META END");
                break;
            }
            default:
                Log.warning("Unknown operation close:" + operation);
                close();
        }
    }

    private int _getState() {
        if (duckdbSession == null) {
            return SessionRemote.STATUS_CLOSED;
        }
        return SessionRemote.STATUS_OK;
    }


    private void sendRows(ResultInterface result, long count) throws IOException {
        int columnCount = result.getVisibleColumnCount();
        boolean lazy = result.isLazy();
//        Session oldSession = lazy ? session.setThreadLocalSession() : null;
        try {
            while (count-- > 0L) {
                boolean hasNext;
                try {
                    hasNext = result.next();
                } catch (Exception e) {
                    transfer.writeByte((byte) -1);
                    sendError(e, false);
                    break;
                }
                if (hasNext) {
                    transfer.writeByte((byte) 1);
                    Value[] values = result.currentRow();
                    for (int i = 0; i < columnCount; i++) {
                        Value v = values[i];
//                        if (lazy && v instanceof ValueLob) {
//                            ValueLob v2 = ((ValueLob) v).copyToResult();
//                            if (v2 != v) {
//                                v = session.addTemporaryLob(v2);
//                            }
//                        }
                        transfer.writeValue(v);
                    }
                } else {
                    transfer.writeByte((byte) 0);
                    break;
                }
            }
        } finally {
//            if (lazy) {
//                session.resetThreadLocalSession(oldSession);
//            }
        }
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    /**
     * Cancel a running statement.
     *
     * @param targetSessionId the session id
     * @param statementId the statement to cancel
     */
    void cancelStatement(String targetSessionId, int statementId) {
        if (Objects.equals(targetSessionId, this.sessionId)) {
            throw DbException.getUnsupportedException("statment cancel  unsupported");
            //todo
//            DuckDBCommand _cmd = (DuckDBCommand) _cache.getObject(statementId, false);
//            _cmd.cancel();
        }
    }

    /**
     * An input stream with a position.
     */
    static class CachedInputStream extends FilterInputStream {

        private static final ByteArrayInputStream DUMMY =
                new ByteArrayInputStream(new byte[0]);
        private long pos;

        CachedInputStream(InputStream in) {
            super(in == null ? DUMMY : in);
            if (in == null) {
                pos = -1;
            }
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            len = super.read(buff, off, len);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read() throws IOException {
            int x = in.read();
            if (x >= 0) {
                pos++;
            }
            return x;
        }

        @Override
        public long skip(long n) throws IOException {
            n = super.skip(n);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        public long getPos() {
            return pos;
        }

    }

}
