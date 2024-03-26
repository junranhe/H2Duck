package hdserver;

import org.duckdb.DuckDBConnection;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.IsolationLevel;
import org.h2.message.DbException;
import org.h2.result.SimpleResult;
import org.h2.util.TimeZoneProvider;
import org.h2.value.*;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public  class DuckDBSessionLocal  {
    Connection conn;
    String dbName;
    SqlTranslator translator;
    ConcurrentHashMap<PreparedStatement, Long> runningStatments;

    long preMysqlClearCacheTime = 0;

    void tryMysqlClearCache() throws SQLException {
        if (DBConfig.getInstance().mysqlIp != null &&
            DBConfig.getInstance().mysqlIp.length() > 0
        ) {
            long t = System.currentTimeMillis();
            if (t - preMysqlClearCacheTime >= DBConfig.getInstance().mysqlSchemaCacheTime * 1000) {
                Statement stmt = conn.createStatement();
                stmt.execute("CALL mysql_clear_cache()");
                stmt.close();
                preMysqlClearCacheTime = t;
                Log.info("clear mysql cache");
            }
        }
    }


    synchronized public void addRunningStatment(PreparedStatement stmt, long delay) {
        runningStatments.put(stmt,System.currentTimeMillis() + delay);
    }

    synchronized public void removeRunningStatment(PreparedStatement stmt) {
        runningStatments.remove(stmt);
    }

    synchronized public void cancelTimeoutStatments()  {
        if (isClosed())
            return;
        long curTime = System.currentTimeMillis();
        ArrayList<PreparedStatement> timeoutStatments
                = new ArrayList<PreparedStatement>();
        for(Map.Entry<PreparedStatement, Long> entry : runningStatments.entrySet()) {
            if (entry.getValue() < curTime)
                timeoutStatments.add(entry.getKey());
        }

        for(PreparedStatement stmt:timeoutStatments) {
            try {
                if (!stmt.isClosed())
                    stmt.cancel();
            }catch (Exception e) {
                DbException.traceThrowable(e);
            }
            runningStatments.remove(stmt);
        }
    }


    public PreparedStatement prepare(String sql) throws SQLException {
        return conn.prepareStatement(sql);
    }

    public Connection getConnection() {
        return conn;
    }

    public SqlTranslator getTranslator() {return translator;}



    DuckDBSessionLocal(DuckDBConnection conn, String databaseName) {
        this.conn = conn;
        this.dbName = databaseName;
        this.translator = new SqlTranslator(DBConfig.getInstance().maxSqlTranCacheSize);
        this.runningStatments = new ConcurrentHashMap();
    }


    public static DuckDBSessionLocal createSession(String dir) {
        try {
            String dbfile = DBConfig.getInstance().baseDir + "/" + DBConfig.getInstance().name;
            DuckDBConnection conn ;
            String[] paths = dir.split("/");
            String schemaName = paths[paths.length -1];

            //mysql agent mode
            if (DBConfig.getInstance().mysqlIp != null && DBConfig.getInstance().mysqlIp.length() > 0) {

                conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                stmt.execute("INSTALL mysql");
                stmt.execute("load mysql");
                String attachSql = "ATTACH 'host=%s user=%s port=%s password=%s database=%s' AS mysqldb (TYPE mysql)";
                stmt.execute(String.format(attachSql,
                        DBConfig.getInstance().mysqlIp,
                        DBConfig.getInstance().mysqlUser,
                        DBConfig.getInstance().mysqlPort,
                        DBConfig.getInstance().mysqlPassword,
                        schemaName
                        ));
                stmt.execute(String.format("use mysqldb.%s", schemaName));
                stmt.execute("SET mysql_experimental_filter_pushdown = true");
                if (DBConfig.getInstance().forbidWrite) {
                    conn.setAutoCommit(false);
                }
            } else {
                conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"+dbfile);
                Statement stmt = conn.createStatement();
                stmt.execute("CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\"");
                conn.setSchema(schemaName);
            }

            return new DuckDBSessionLocal(conn, schemaName);
        } catch (Exception e) {
            throw HdException.convert(e, "createSession error");

        }
    }


    public void close() {
        try {
            conn.close();
            conn = null;
            runningStatments = null;
        } catch (Exception e) {
            throw DbException.convert( e);
        }
    }

    //todo
    public boolean hasPendingTransaction() {
        return !getAutoCommit();
    }

    public void checkClosed(String s) {
        if (isClosed()) {
            throw HdException.get(s);
        }
    }

    public boolean getAutoCommit() {
        checkClosed("getAutoCommit");
        try {
            return conn.getAutoCommit();
        }
        catch(Exception e)
        {
            throw DbException.convert( e);
        }
    }

    public void setAutoCommit(boolean b)  {
        if (DBConfig.getInstance().forbidWrite && b)
            throw HdException.get("Config Forbid auto commit");
        try {
            boolean isAuto = getAutoCommit();
            if (isAuto != b) {
                conn.setAutoCommit(b);
            }
        } catch(Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetaSchemas() {
        checkClosed("getMetaSchemas");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(dbMetaData.getSchemas());
        }catch (Exception e) {
            throw DbException.convert( e);
        }
    }



    private SimpleResult getMetaCatalogs() {
        checkClosed("getMetaCatalogs");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(dbMetaData.getCatalogs());
        }catch (Exception e) {
            throw DbException.convert( e);
        }
    }

    private SimpleResult getMetaTables(Value[] args) {
        checkClosed("getMetaTables");
        try {
            if (args.length != 4)
                throw DbException.get(ErrorCode.GENERAL_ERROR_1, "getMetaTables args size is not 4");
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(
                    dbMetaData.getTables(
                            args[0].getString(),
                            args[1].getString(),
                            args[2].getString(),
                            toStringArray(args[3])));
        }catch (Exception e) {
            throw DbException.convert( e);
        }
    }

    private static String[] toStringArray(Value value) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        Value[] list = ((ValueArray) value).getList();
        int l = list.length;
        String[] result = new String[l];
        for (int i = 0; i < l; i++) {
            result[i] = list[i].getString();
        }
        return result;
    }

    private SimpleResult getMetaTableTypes() {
        checkClosed("getMetaTableTypes");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(dbMetaData.getTableTypes());
        }catch (Exception e) {
            throw DbException.convert( e);
        }
    }

    private SimpleResult getMetaColumns(Value[] args) {
        checkClosed("getMetaColumns");
        try {
            if (args.length != 4)
                throw DbException.get(ErrorCode.GENERAL_ERROR_1, "getMetaColumns args size is not 4");
            DatabaseMetaData dbMetaData = conn.getMetaData();
            SimpleResult res =  resultSetToSimpleResult(
                    dbMetaData.getColumns(
                            args[0].getString(),
                            args[1].getString(),
                            args[2].getString(),
                            args[3].getString()));
            return res;
        }catch (Exception e) {
            throw DbException.convert( e);
        }
    }

    private SimpleResult getMetaPrimaryKeys(Value[] args) {
        checkClosed("getMetaPrimaryKeys");
        try {
            if (args.length != 3)
                throw DbException.get(ErrorCode.GENERAL_ERROR_1, "getMetaPrimaryKeys args size is not 3");
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(
                    dbMetaData.getPrimaryKeys(
                            args[0].getString(),
                            args[1].getString(),
                            args[2].getString()));
        }catch (Exception e) {
            throw DbException.convert( e);
        }
    }

    //duckdb 原生jdbc不支持
    private SimpleResult getMetaVersionColumns(Value[] args) {
        checkClosed("getMetaVersionColumns");
        try {
            if (args.length != 3)
                throw DbException.get(ErrorCode.GENERAL_ERROR_1, "getMetaVersionColumns args size is not 3");
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(
                    dbMetaData.getVersionColumns(
                            args[0].getString(),
                            args[1].getString(),
                            args[2].getString()));
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public String getDbName() {
        return this.dbName;
    }

    private SimpleResult getMetaDatabaseProductVersion() {
        checkClosed("getMetaDatabaseProductVersion");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return result(ValueVarchar.get(Constants.FULL_VERSION));
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetaSQLKeywords() {
        checkClosed("getMetaSQLKeywords");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return result(ValueVarchar.get(dbMetaData.getSQLKeywords()));
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetaNumericFunctions() {
        checkClosed("getMetaNumericFunctions");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return result(ValueVarchar.get(dbMetaData.getNumericFunctions()));
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetaSearchStringEscape() {
        checkClosed("getMetaSearchStringEscape");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return result(ValueNull.INSTANCE);
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetaTypeInfo() {
        checkClosed("getMetaTypeInfo");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return resultSetToSimpleResult(dbMetaData.getTypeInfo());
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetaDatabaseMajorVersion() {
        checkClosed("getMetaDatabaseMajorVersion");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return result( ValueInteger.get(Constants.VERSION_MAJOR));
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private SimpleResult getMetagetDatabaseMinorVersion() {
        checkClosed("getMetagetDatabaseMinorVersion");
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            return result( ValueInteger.get(Constants.VERSION_MINOR));
        }catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private static SimpleResult result(Value v) {
        SimpleResult res = new SimpleResult();
        res.addColumn("RESULT", v.getType());
        res.addRow(new Value[]{v});
        return res;
    }

    public SimpleResult getJdbcMetaData(int code, Value[] args) {
        checkClosed("getJdbcMetaData");

        switch (code) {
            case 1:
                return getMetaDatabaseProductVersion();
            case 2:
                return getMetaSQLKeywords();
            case 3:
                return getMetaNumericFunctions();
            case 7:
                return getMetaSearchStringEscape();
//            case 8:
            //GET_PROCEDURES_3
//                return null;
            case 10:
                return getMetaTables(args);
            case 11:
                return getMetaSchemas() ;
            case 12:
                return getMetaCatalogs();
            case 13:
                return getMetaTableTypes();
            case 14:
                return getMetaColumns(args);
            case 18:
                return getMetaVersionColumns(args);
            case 19:
                return getMetaPrimaryKeys(args);
//            case 20:
            //GET_IMPORTED_KEYS_3
//                return null;
//            case 21:
            //GET_EXPORTED_KEYS_3
//                return null;
            case 23:
                return getMetaTypeInfo();
                //duckdb 驱动报错
//            case 24: //todo duckdb 原生不支持,需要自己实现
            //GET_INDEX_INFO_5
//                return getMetaIndexInfo(args);
            case 29:
                return getMetaDatabaseMajorVersion();
            case 30:
                return getMetagetDatabaseMinorVersion();
            default:
                Log.warning("getJdbcMetaData Error:" + code);
                throw DbException.getUnsupportedException("META " + code);
        }

    }

    public static SimpleResult resultSetToSimpleResult (ResultSet resultSet) {
        SimpleResult rs = new SimpleResult();
        try {
            ResultSetMetaData rsMeta = resultSet.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            for (int i= 1; i <= columnCount; i++) {
                int valueType = DataType.convertSQLTypeToValueType(rsMeta.getColumnType(i));
                //javaobject 主要转换为整数才能被orm识别转换
                if (valueType == Value.JAVA_OBJECT)
                    valueType = Value.BIGINT;
                TypeInfo info = TypeInfo.getTypeInfo(valueType);
                rs.addColumn(rsMeta.getColumnName(i),info );
            }

            while (resultSet.next()) {
                Value[] values = new Value[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    int valueType = DataType.convertSQLTypeToValueType(rsMeta.getColumnType(i));
                    Value v = null;
                    switch (valueType) {
                        case Value.BOOLEAN:
                            v = ValueBoolean.get(resultSet.getBoolean(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.INTEGER:
                            v = ValueInteger.get(resultSet.getInt(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.BIGINT:
                            v = ValueBigint.get(resultSet.getLong(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.DOUBLE:
                            v = ValueDouble.get(resultSet.getDouble(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.VARCHAR:
                            v = (resultSet.getString(i) != null) ? ValueVarchar.get(resultSet.getString(i)):ValueNull.INSTANCE;
                            break;
                        case Value.VARCHAR_IGNORECASE:
                            v = (resultSet.getString(i) != null) ? ValueVarcharIgnoreCase.get(resultSet.getString(i)):ValueNull.INSTANCE;
                            break;
                        case Value.NUMERIC:
                            v = (resultSet.getBigDecimal(i) != null) ? ValueNumeric.get(resultSet.getBigDecimal(i)):ValueNull.INSTANCE;
                            break;
                        case Value.TINYINT:
                            v = ValueTinyint.get(resultSet.getByte(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.SMALLINT:
                            v = ValueSmallint.get(resultSet.getShort(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.REAL:
                            v = ValueReal.get(resultSet.getFloat(i));
                            if (resultSet.wasNull())
                                v = ValueNull.INSTANCE;
                            break;
                        case Value.DATE:
                            try {
                                v = (resultSet.getDate(i) != null) ?
                                        ValueDate.fromDateValue(resultSet.getDate(i).getTime()) : ValueNull.INSTANCE;
                            } catch (Exception e) {
                                throw DbException.convert(e);
                            }
                            break;
                        case Value.TIME:
                            try {
                                v = (resultSet.getTime(i) != null) ?
                                        ValueTime.fromNanos(resultSet.getTime(i).getTime()) : ValueNull.INSTANCE;
                            } catch (Exception e) {
                                throw DbException.convert(e);
                            }
                            break;
                        case Value.TIMESTAMP:
                            try {
                                // todo 寻找更高效的时间格式转换，用datavalue的方式会报异常
                                v = (resultSet.getTimestamp(i) != null) ?
                                        ValueTimestamp.parse(resultSet.getTimestamp(i).toString(), null) : ValueNull.INSTANCE;
                            } catch (Exception e) {
                                throw DbException.convert(e);
                            }
                            break;
                        case Value.JAVA_OBJECT:
                            Object obj = resultSet.getObject(i);
                            if (obj == null) {
                                v = ValueNull.INSTANCE;
                            }else if (obj instanceof BigInteger){
                                v = ValueBigint.get(((BigInteger) obj).longValue());
                            } else if (obj instanceof Long) {
                                v = ValueBigint.get(((Long) obj).longValue());
                            } else if (obj instanceof Integer) {
                                v = ValueBigint.get(((Integer) obj).longValue());
                            } else if (obj instanceof Short) {
                                v = ValueBigint.get(((Short) obj).longValue());
                            } else if (obj instanceof Byte) {
                                v = ValueBigint.get(((Byte) obj).longValue());
                            } else {
                                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                                        "resultSetToSimpleResult javaobject can not change to bigint:"
                                                +obj.getClass().toString());
                            }
                            break;
                        default:
                            throw DbException.getUnsupportedException("resultSetToSimpleResult unknow type:" + valueType);
                    }

                    values[i-1] = v;
                }

                if (rs.getRowCount() > 100000)
                    throw HdException.get("result set rows more than 100000:%s");
                rs.addRow(values);
            }

        } catch (Exception e) {
            throw DbException.convert(e);
        }

        return rs;
    }

    public IsolationLevel getIsolationLevel() {
        checkClosed("getIsolationLevel");
        return IsolationLevel.REPEATABLE_READ;
    }

    public void commit(boolean ddl) {
        checkClosed("commit");
        if (!getAutoCommit()) {
            try {
                conn.commit();
            }catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    public boolean isClosed()  {
        try {
            return conn.isClosed();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public void setTimeZone(TimeZoneProvider provider) {
    }

    public static boolean isValueString(int type) {
        switch (type) {
            case Value.CHAR:
            case Value.VARCHAR:
            case Value.CLOB:
            case Value.VARCHAR_IGNORECASE:
                return true;
            default:
                return false;
        }
    }

    public static boolean isValueNumeric(int type) {
        switch (type) {
            case Value.TINYINT:
            case Value.SMALLINT:
            case Value.INTEGER:
            case Value.BIGINT:
            case Value.NUMERIC:
            case Value.REAL:
            case Value.DOUBLE:
            case Value.DECFLOAT:
                return true;
            default:
                return false;
        }
    }


    public static void setParameterValue(PreparedStatement prepared, ParameterMetaData pMeta, int i, Value v)  {
        try {
            int valueType = v.getValueType();
            int paramType = DataType.convertSQLTypeToValueType(pMeta.getParameterType(i));
            if (paramType != Value.NULL && paramType != valueType)
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "setParameterValue error:" +  v.toString()
                                + " valueType:"+ valueType
                                + " paramType:" + paramType
                );

            switch (valueType) {
                case Value.BOOLEAN:
                    prepared.setBoolean(i, v.getBoolean());
                    break;
                case Value.INTEGER:
                    prepared.setInt(i, v.getInt());
                    break;
                case Value.BIGINT:
                    prepared.setLong(i, v.getLong());
                    break;
                case Value.DOUBLE:
                    prepared.setDouble(i, v.getDouble());
                    break;
                case Value.VARCHAR:
                case Value.VARCHAR_IGNORECASE:
                    prepared.setString(i, v.getString());
                    break;
                case Value.NUMERIC:
                    prepared.setBigDecimal(i, v.getBigDecimal());
                    break;
                case Value.TINYINT:
                    prepared.setByte(i, v.getByte());
                    break;
                case Value.SMALLINT:
                    prepared.setShort(i, v.getShort());
                    break;
                case Value.REAL:
                    prepared.setFloat(i, v.getFloat());
                    break;
                case Value.DATE:
                    ValueDate vDate = (ValueDate) v;
                    java.sql.Date d = new java.sql.Date(vDate.getDateValue());
                    prepared.setDate(i, d);
                    break;
                case Value.TIME:
                    ValueTime vTime = (ValueTime) v;
                    java.sql.Time t = new java.sql.Time(vTime.getNanos());
                    prepared.setTime(i, t);
                    break;
                // todo
                case Value.TIMESTAMP:
                    ValueTimestamp vTimestamp = (ValueTimestamp) v;
                    java.sql.Timestamp ts = new java.sql.Timestamp(vTimestamp.getTimeNanos());
                    prepared.setTimestamp(i, ts);
                    break;
                case Value.NULL:
                    prepared.setNull(i, pMeta.getParameterType(i));
                    break;
                case Value.ARRAY:
                    //复杂结构转化为string做占位符
                    prepared.setString(i,v.toString());
                    break;
                default:
                    throw DbException.get(ErrorCode.GENERAL_ERROR_1, "setParameterValue ValueType ERROR:"+valueType);
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

}
