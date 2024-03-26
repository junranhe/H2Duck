package hdserver;

import org.duckdb.DuckDBConnection;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.message.DbException;
import org.h2.result.SimpleResult;
import org.h2.value.*;
import java.sql.*;
import java.util.HashMap;

public  class DuckDBCommand  {

    protected DuckDBSessionLocal session;

    protected PreparedStatement prepared;

    protected ParameterMetaData pMeta;

    protected HashMap<Integer,Value> paramMap;
    protected CommandInfo commandInfo;

    public  static DuckDBCommand newInstance(DuckDBSessionLocal session, String sql) {
        //todo 产生了两次sql翻译
        long tTranBegin = System.currentTimeMillis();
        CommandInfo info = session.getTranslator().tran(sql);

        long tTranEnd = System.currentTimeMillis();
//        System.out.println("sql trans:(ms):" + (tTranEnd - tTranBegin));

        if (info == null) {
            throw HdException.get( "sql trans inner engine sql error:" + sql);
        }

        if (!info.getIsQuery() &&
                DBConfig.getInstance().forbidWrite
                && info.getType() != CommandSqlType.set
                && info.getType() != CommandSqlType.use) {
            throw HdException.get( "formit write data error:" + sql);
        }

        try {
            PreparedStatement prepared;
            prepared = session.getConnection().prepareStatement(info.getSql());
            long tPrepared = System.currentTimeMillis();

            ParameterMetaData pMeta = prepared.getParameterMetaData();
            long tMeta = System.currentTimeMillis();
            return new DuckDBCommand(session, prepared, pMeta, info.getSql(), info);
        } catch (Exception e) {
            throw HdException.convert(e, "DuckDBCommand newInstance error");
        }
    }

    public void tryReset() {
        if (!isClosed())
            return;
        if (session.isClosed())
            return;
        try {

            PreparedStatement newPrepared = session.getConnection().prepareStatement(commandInfo.getSql());
            ParameterMetaData newMeta = newPrepared.getParameterMetaData();
            prepared = newPrepared;
            pMeta = newMeta;
            paramMap = new HashMap<Integer, Value>();
        } catch (Exception e) {
            throw DbException.convert(e);
        }

    }

    public String getSql() {
        return commandInfo.getSql();
    }

    private void checkClosed(String s) {
        if (isClosed()) {
            System.out.println("checkclose fail:" + s);
            throw DbException.get(ErrorCode.GENERAL_ERROR_1, s);
        }
    }

    public boolean isClosed() {
        try {
            if (session == null || session.isClosed())
                return true;
            return (prepared == null || prepared.isClosed());
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private DuckDBCommand(DuckDBSessionLocal session, PreparedStatement prepared,  ParameterMetaData pMeta, String sql, CommandInfo info) {
        this.session = session;
        this.prepared = prepared;
        this.pMeta = pMeta;
        this.commandInfo = info;
        this.paramMap = new HashMap<Integer, Value>();
    }


    public boolean isQuery() {
        return this.commandInfo.getIsQuery();
    }

    public int getCommandType() {
        if (isQuery())
            return CommandInterface.SELECT;
        return CommandInterface.UNKNOWN;
    }

    public CommandSqlType getSqlType() {
        return commandInfo.getType();
    }


    @Override
    public String toString() {
        return this.commandInfo.getSql();
    }


    /**
     * Get the list of parameters.
     *
     * @return the list of parameters
     */
    public  ParameterMetaData getParameterMetaData() {
        return pMeta;
    }

    public SimpleResult executeQuery()  {
        checkClosed("executeQuery");
        if (!isQuery()) {
            System.out.println(commandInfo.getSql() + " command type:" + commandInfo.getType());
            throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY, commandInfo.getSql());
        }
        try {
            session.tryMysqlClearCache();
            session.addRunningStatment(prepared, DBConfig.getInstance().sqlTimeout * 1000);

            ResultSet res =  prepared.executeQuery();
            return DuckDBSessionLocal.resultSetToSimpleResult(res);

        } catch (Exception e) {
            System.out.println("query error sql:" + commandInfo.getSql());
            System.out.println("query error msg:" + e.getMessage());
            if (e.getMessage().startsWith("INTERRUPT Error"))
                throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED,"executeQuery:" + commandInfo.getSql());
            throw DbException.convert(e);
        } finally {
            session.removeRunningStatment(prepared);
        }
    }


    public int executeUpdate() {
        checkClosed("executeUpdate");
        if (isQuery() )
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY, commandInfo.getSql());
        try {
            session.tryMysqlClearCache();
            if (commandInfo.getType() == CommandSqlType.commit) {
                session.getConnection().commit();
                return 0;
            }
            if (commandInfo.getType() == CommandSqlType.rollback) {
                session.getConnection().rollback();
                return 0;
            }

            if (commandInfo.getType() == CommandSqlType.set) {
                Log.info("executeUpdate Ignore SetStatment:" +commandInfo.getSql());
                return 0;
            }

            session.addRunningStatment(prepared, DBConfig.getInstance().sqlTimeout * 1000);
            return prepared.executeUpdate();
        } catch (Exception e) {
            System.out.println("update error sql:" + commandInfo.getSql());
            if (e.getMessage().startsWith("INTERRUPT Error"))
                throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED,"executeQuery:" + commandInfo.getSql());
            throw DbException.convert(e);
        } finally {
            session.removeRunningStatment(prepared);
        }
    }

    public void clearParameters() {
        System.out.println("clearParameters");
        try {
            prepared.clearParameters();
        } catch (Exception e) {
            System.out.println("clear parameters error:" + e.getMessage());
        }
    }

    public void close() {
        checkClosed("close command");
        try {
            prepared.close();
            prepared = null;
            session = null;
            pMeta = null;
            paramMap = null;
            commandInfo = null;
        } catch(Exception e) {
            throw DbException.convert(e);
        }
    }

    public void setParameterValue(int i, Value v) {
        checkClosed("DuckDBCommand setParameterValue");
        DuckDBSessionLocal.setParameterValue(prepared, pMeta, i, v);
        paramMap.put(i,v);
    }

}

