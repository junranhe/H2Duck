package hdserver;

import java.util.ArrayList;
import java.util.HashMap;

public class CommandInfo {
    private CommandSqlType type;
    private boolean isQuery;
    private String sql;

    private ArrayList<String> args = new ArrayList<String>();

    private String originSql;

    private HashMap<String, String> context;

    public CommandInfo(CommandSqlType _type,
                       boolean _isQuery, String _sql) {
        this.type = _type;
        this.isQuery = _isQuery;
        this.sql = _sql;
        this.context = new HashMap<String, String>();
    }

    public CommandInfo(CommandSqlType _type, boolean _isQuery, String _sql, String _originSql) {
        this.type = _type;
        this.isQuery = _isQuery;
        this.sql = _sql;
        this.originSql = _originSql;
        this.context = new HashMap<String, String>();
    }

    public CommandInfo(CommandSqlType _type, boolean _isQuery, String _sql, String _originSql,
                        HashMap<String, String> _context) {
        this.type = _type;
        this.isQuery = _isQuery;
        this.sql = _sql;
        this.originSql = _originSql;
        this.context = _context;
    }

    public CommandInfo withContext(HashMap<String, String> _context) {
        return new CommandInfo(this.type, this.isQuery, this.sql,this.originSql,_context);
    }

    public CommandSqlType getType()  {return type;}
    public boolean getIsQuery() {return isQuery;}
    public String getSql() {return sql;}
    public ArrayList<String> getArgs() {return args;}
    public String getOriginSql() {return originSql;}

    public HashMap<String, String> getContext() {return context;}
}
