package hdserver;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.function.CreateFunction;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.procedure.CreateProcedure;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;


public class SqlTranslator {
    public static class ScriptStatmentAttr {
        final private boolean isFunc;
        final private boolean isReplace;
        final private String scriptType;
        final private String name;
        final private String content;
        final private ArrayList<Map.Entry<String, String>> args;
        public String getStringArgs() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < args.size(); i++) {
                if (i != 0)
                    sb.append(',');
                sb.append(args.get(i).getKey());
                sb.append(' ');
                sb.append(args.get(i).getValue());
            }
            return sb.toString();
        }
        public ScriptStatmentAttr(boolean _isFunc
                , boolean _isReplace
                , String _scriptType
                ,String _name
                ,ArrayList<Map.Entry<String, String>> _args
                , String _content) {
            isFunc = _isFunc;
            isReplace = _isReplace;
            scriptType = _scriptType;
            name = _name;
            args = _args;
            content = _content;
        }
        public boolean getIsFunc() {return isFunc;}
        public boolean getIsReplace() {return isReplace;}
        public String getScriptType() {return scriptType;}
        public String getName() {return name;}
        public String getContent() {return content;}
    }

    public static class ViewStatmentAttr {
        final private boolean isReplace;
        final private String viewCode;
        final private String content;
        public ViewStatmentAttr(
                boolean _isReplace
                ,String _viewCode
                , String _content) {
            isReplace = _isReplace;
            viewCode = _viewCode;
            content = _content;
        }
        public boolean getIsReplace() {return isReplace;}
        public String getViewCode() {return viewCode;}
        public String getContent() {return content;}
    }

    public  static class MyDialect extends PostgresqlSqlDialect {
        public MyDialect(Context context) {
            super(context);
        }

        @Override
        public void quoteStringLiteral( StringBuilder buf, String charsetName, String  val) {
            buf.append(literalQuoteString);
            buf.append(val.replace(literalEndQuoteString,literalEscapedQuote));
            buf.append(literalEndQuoteString);
        }
    }


    private final static String sysGetNameValue = "SELECT SETTING_NAME, SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS";

    private final static String sysGetNameValueReplace =
            "SELECT SETTING_NAME, SETTING_VALUE FROM (VALUES ('MODE', 'MySQL'),('TIME ZONE', 'Asia/Shanghai')) AS T(SETTING_NAME, SETTING_VALUE)  ";

    private final static String sysGetValue = "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS";

    private final static String sysGetValueReplace =
            "SELECT SETTING_VALUE FROM (VALUES  ('MODE', 'MySQL'),('TIME ZONE', 'Asia/Shanghai')) AS T(SETTING_NAME, SETTING_VALUE)  ";

    private final static String sysGetIsolationLevel = "SELECT ISOLATION_LEVEL FROM INFORMATION_SCHEMA.SESSIONS WHERE SESSION_ID = SESSION_ID()";

    private final static String sysGetIsolationLevelReplace = "SELECT 'REPEATABLE READ' as ISOLATION_LEVEL";

    public final static String sysSetIsolationLevel = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ";

    public final static String sysGetSchema = "SELECT SCHEMA()";

    public final static String sysGetSchemaReplace = "SELECT current_schema()";

    public final static String sysCallDatabase = "CALL DATABASE()";

    public final static String sysCallReadonly = "CALL READONLY()";

    final private LruCache<String, CommandInfo> lruSqlCache;

    final private SqlParser.Config mysqlConfig;
    final private CCJSqlParserManager jsqlParser;
    final private MyDialect toPgDialect;

    public CCJSqlParserManager getJsqlParser() {return jsqlParser;}


    public static SqlParser.Config createMysqlConfig() {
        return  SqlParser.config()
                .withQuoting(Quoting.BACK_TICK)
                .withQuotedCasing(Casing.UNCHANGED)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withConformance(SqlConformanceEnum.BIG_QUERY)
                .withCaseSensitive(false)
//                .withCharLiteralStyles(Lex.MYSQL.charLiteralStyles);
                .withCharLiteralStyles(Lex.BIG_QUERY.charLiteralStyles);
    }

    public static MyDialect createDialect() {
        SqlDialect.Context toPgContext = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
                .withIdentifierQuoteString("\"");
        return new MyDialect(toPgContext);
    }

    public SqlTranslator(int cacheSize) {
        lruSqlCache = new LruCache<String, CommandInfo>(cacheSize);
        mysqlConfig = createMysqlConfig();
        jsqlParser = new CCJSqlParserManager();
        toPgDialect = createDialect();
    }

    public CommandInfo systemTran(String sql) {
        String newSql = null;
        if (sql.startsWith(sysGetNameValue)) {
            newSql = sysGetNameValueReplace + sql.substring(sysGetNameValue.length());
        } else if (sql.startsWith(sysGetValue)) {
            newSql = sysGetValueReplace + sql.substring(sysGetValue.length());
        } else if (sql.equals(sysGetIsolationLevel)) {
            newSql = sysGetIsolationLevelReplace;
        } else if (sql.equals(sysGetSchema)) {
            newSql = sysGetSchemaReplace;
        } else if (sql.equals(sysCallDatabase)) {
            newSql = "SELECT '" + "CallSyStem"  + "' AS RESULT" ;
        } else if (sql.equalsIgnoreCase(sysCallReadonly)) {
            newSql = "SELECT 0 AS RESULT";
        }

        if (newSql == null)
            return null;
        return new CommandInfo(CommandSqlType.select, true, newSql);
    }

    private CommandInfo directTran(String sql) {
        if (!sql.startsWith("/*direct_tran*/"))
            return null;

        try {
            StringReader reader = new StringReader(sql);
            Statement stmt = jsqlParser.parse(reader);


            CommandSqlType type = CommandSqlType.other;
            boolean isQuery=false;
            if (stmt instanceof CreateTable) {
                type = CommandSqlType.create_table;
            } else if (stmt instanceof Execute) {
                type = CommandSqlType.execute;
            } else if (stmt instanceof RollbackStatement) {
                type = CommandSqlType.rollback;
            } else if (stmt instanceof Commit) {
                type = CommandSqlType.commit;
            } else if (stmt instanceof CreateIndex) {
                type = CommandSqlType.create_index;
            } else if (stmt instanceof Select) {
                type = CommandSqlType.select;
                isQuery= true;
            } else if (stmt instanceof Update) {
                type = CommandSqlType.update;
            } else if (stmt instanceof Insert) {
                type = CommandSqlType.insert;
            } else if (stmt instanceof Delete) {
                type = CommandSqlType.delete;
            } else if (stmt instanceof SetStatement) {
                type = CommandSqlType.set;
            } else if (stmt instanceof UseStatement) {
                type = CommandSqlType.use;
            } else if (stmt instanceof  CreateFunction) {
                type = CommandSqlType.create_function;
            } else if (stmt instanceof  CreateProcedure) {
                type = CommandSqlType.create_procedure;
            } else if (stmt instanceof  Alter) {
                type = CommandSqlType.alter;
            }
            if (type == CommandSqlType.other)
                return null;

            return new CommandInfo(type, isQuery, sql);
        }catch (Exception e) {
            Log.warning("directTran parse error:"+sql);
            Log.warning("directTran parse error msg::"+e.getMessage());
            return null;
        }
    }

    final private static String USER_CONTEXT_PREFIX = "/*user_context(";
    final private static String USER_CONTEXT_SUFFIX = ")*/";

    public CommandInfo tran(String sql) {
        final String prefix= USER_CONTEXT_PREFIX;
        final String suffix = USER_CONTEXT_SUFFIX;
        String newSql = sql;
        HashMap<String, String> context = new HashMap<String, String>();
        boolean hasUserContext = false;
        if (sql.startsWith(prefix) && sql.indexOf(suffix) > prefix.length()) {
            String ctxData = sql.substring(prefix.length(), sql.indexOf(suffix));
            HashMap<String, String> tempContext = new HashMap<String, String>();
            boolean checkHint = true;
            for (String s : ctxData.split(",")) {
                String[] kv = s.split("=");
                if (kv.length != 2) {
                    checkHint = false;
                    break;
                }
                tempContext.put(kv[0].trim(), kv[1].trim());
            }
            if (checkHint) {
                newSql = sql.substring(sql.indexOf(suffix) + suffix.length());
                context = tempContext;
            }

            hasUserContext = true;
        }

        CommandInfo res = lruSqlCache.get(newSql);
        if (res != null) {
            //添加上下文但是不能改sql内容
            return res.withContext(context);
        }
        res = innerTran(newSql, hasUserContext);
        if (res != null && res.getType() != CommandSqlType.other) {
            lruSqlCache.put(newSql,res);
            //添加上下文但是不能改sql内容
            return res.withContext(context);
        }

        throw HdException.get("tran sql failed");
    }

    public CommandInfo innerTran(String sql, boolean hasUserContext) {

        if (sql.equals(sysSetIsolationLevel))
            throw HdException.getUnsupportedException("SetIsolationLevel Unsupported");

        CommandInfo res = directTran(sql);
        if (res != null)
            return res;
        res = systemTran(sql);
        if (res != null)
            return res;
        res = mysqlToPgsql(sql, hasUserContext);
        if (res != null)
            return res;
        return  parseAntherSql(sql);
    }

    public static class SqlConvertor extends SqlBasicVisitor<SqlNode> {
        private SqlBasicCall createCastDate(SqlNode node) {
            SqlCastFunction castFunc = new SqlCastFunction();
            SqlAlienSystemTypeNameSpec typeNameSpec = new SqlAlienSystemTypeNameSpec(
                    "DATE", SqlTypeName.DATE, SqlParserPos.ZERO);
            SqlDataTypeSpec typeSpec = new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
            SqlBasicCall call = new SqlBasicCall(castFunc, new SqlNode[]{node, typeSpec}, SqlParserPos.ZERO);
            return call;
        }


        @Override
        public SqlNode  visit(SqlLiteral literal) {
            if (literal instanceof SqlCharStringLiteral) {
                //转换特殊转义字符串为concat函数
                return toConcatSQLExpress((SqlCharStringLiteral) literal);
            }
            return super.visit(literal);
        }

        public SqlBasicCall createFunCall(String name, SqlNode ... ops) {
            SqlOperator op = new SqlUnresolvedFunction(
                    new SqlIdentifier(Arrays.asList(new String[]{name}),SqlParserPos.ZERO),
                    null, null, null, null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
            SqlBasicCall call = new SqlBasicCall(op,ops, SqlParserPos.ZERO);
            return call;
        }

        public SqlIdentifier createIdentifier(String name) {
            return new SqlIdentifier(Arrays.asList(new String[]{name}),SqlParserPos.ZERO);
        }

        public SqlBasicCall monthEnd() {
            return createFunCall("last_day", createIdentifier("current_date"));
        }

        public SqlBasicCall monthBegin() {
            return createFunCall("date_trunc",
                    SqlLiteral.createCharString("month",SqlParserPos.ZERO),
                    createIdentifier("current_date"));
        }

        public SqlBasicCall dayBegin() {
            return createFunCall("date_trunc",
                    SqlLiteral.createCharString("day",SqlParserPos.ZERO),
                    createIdentifier("current_date"));
        }

        public SqlBasicCall dayEnd() {
            return createFunCall("date_trunc",
                    SqlLiteral.createCharString("day",SqlParserPos.ZERO),
                    createIdentifier("current_date"));
        }

        public SqlBasicCall yearBegin() {
            return createFunCall("date_trunc",
                    SqlLiteral.createCharString("year",SqlParserPos.ZERO),
                    createIdentifier("current_date"));
        }

        public SqlBasicCall yearEnd() {
            return createFunCall("make_date",
                    createFunCall("year", createIdentifier("current_date")),
                    SqlLiteral.createExactNumeric("12",SqlParserPos.ZERO),
                    SqlLiteral.createExactNumeric("31",SqlParserPos.ZERO)
                    );
        }

        public SqlCase createCase(SqlNode value, SqlNode[] whenList, SqlNode[] thenList, SqlNode elseExpr) {
            return new SqlCase(SqlParserPos.ZERO, value,
                    new SqlNodeList(Arrays.asList(whenList) , SqlParserPos.ZERO),
                    new SqlNodeList(Arrays.asList(thenList), SqlParserPos.ZERO),
                    elseExpr);
        }

        public SqlCall convertDateRangeFunc(SqlBasicCall call, boolean isBegin) {
            if (call.getOperands() == null || call.getOperands().length != 1) {
                throw HdException.get("convertDateRangeFunc DateRange args count  not 1:"+call.toString());
            }

            SqlNode[] whenList = new SqlNode[] {
                    SqlLiteral.createCharString("day",SqlParserPos.ZERO),
                    SqlLiteral.createCharString("month",SqlParserPos.ZERO),
                    SqlLiteral.createCharString("year",SqlParserPos.ZERO)
            };
            SqlNode elseExpr = SqlLiteral.createNull(SqlParserPos.ZERO);
            SqlNode[] thenList = null;
            if (isBegin)
                thenList = new SqlNode[] { dayBegin(), monthBegin(), yearBegin()};
            else
                thenList = new SqlNode[] { dayEnd(), monthEnd(), yearEnd()};

            return createCase(call.getOperands()[0], whenList, thenList, elseExpr
            );
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlBasicCall &&
                    call.getOperator() instanceof  SqlUnresolvedFunction) {
                //时间函数变换
                SqlBasicCall baseCall = (SqlBasicCall)call;
                SqlUnresolvedFunction func = (SqlUnresolvedFunction) call.getOperator();
                String funcName = func.getName().toLowerCase();

                if (funcName.equals("adddate") || funcName.equals("date_add")) {
                    baseCall.setOperator(SqlStdOperatorTable.DATETIME_PLUS);
                    //强制类型转换，避免定位不到操作符
                    if (baseCall.getOperands().length >= 1) {
                        SqlNode firstArg = baseCall.getOperands()[0];
                        baseCall.setOperand(0, createCastDate(firstArg));
                    }
                }else if (funcName.equals("subdate") || funcName.equals("date_sub"))    {
                    baseCall.setOperator(SqlStdOperatorTable.MINUS_DATE);
                    //强制类型转换，避免定位不到操作符
                    if (baseCall.getOperands().length >= 1) {
                        SqlNode firstArg = baseCall.getOperands()[0];
                        baseCall.setOperand(0, createCastDate(firstArg));
                    }
                } else if (funcName.equals("date_format")) {
                    SqlOperator op = new SqlUnresolvedFunction(
                            new SqlIdentifier(Arrays.asList(new String[]{"strftime"}),SqlParserPos.ZERO),
                            null, null, null, null,
                            SqlFunctionCategory.USER_DEFINED_FUNCTION);
                    baseCall.setOperator(op);
                } else if (funcName.equals("date_begin") ||  funcName.equals("date_end"))  {
                    SqlCall caseCall = convertDateRangeFunc(baseCall, funcName.equals("date_begin"));
                    return caseCall;
                }
            } else if (call instanceof SqlBasicCall &&
                    call.getOperator() instanceof  SqlInternalOperator) {
                // 检查 时间 internal xxx day 表达式合法性
                SqlNode[] operands = ((SqlBasicCall) call).getOperands();
                if (operands.length >= 2 && operands[0] instanceof SqlNumericLiteral) {
                    SqlNumericLiteral n = (SqlNumericLiteral)operands[0];
                    if (n.getValue() instanceof  BigDecimal ){
                        BigDecimal bigDecimal = (BigDecimal) n.getValue();
                        if (bigDecimal.toBigInteger().longValue() < 0) {
                            throw HdException.get("interval value error");
                        }
                    }
                }
            }

            List<SqlNode> nodeList = call.getOperandList();

            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode operand = nodeList.get(i);
                if (operand != null) {
                    SqlNode newNode = operand.accept(this);
                    if (newNode != null && newNode != operand){
                        try {
                            call.setOperand(i, newNode);
                        }catch (Exception e) {
                            System.out.println("error");
                        }
                    }
                }

            }
            return null;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                if (node != null) {
                    SqlNode newNode = node.accept(this);
                    if (newNode != null && newNode != node){
                        nodeList.set(i, newNode);
                    }
                }
            }
            return null;
        }
    }

    private  CommandInfo mysqlToPgsql(String sql, boolean hasUserContext)  {
        try {
            sql = sql.trim();
            if (sql.endsWith(";"))
                sql = sql.substring(0, sql.length()-1).trim();
            SqlParser sqlParser = SqlParser.create(sql, mysqlConfig);
            SqlNode sqlNode = sqlParser.parseQuery();

            //call xxx() concat 替换特殊处理逻辑
            if (sqlNode.getKind() == SqlKind.PROCEDURE_CALL) {
                return getExecuteCalcite(sqlNode, sql);
            }

            if ( hasUserContext &&
                    (       sqlNode.getKind() == SqlKind.SELECT ||
                            sqlNode.getKind() == SqlKind.ORDER_BY ||
                            sqlNode.getKind() == SqlKind.WITH)) {
                return new CommandInfo(CommandSqlType.select,true,sql, sql);
            }

            sqlNode.accept(new SqlConvertor());

            Log.info("test");
            String res = sqlNode.toSqlString(config -> config.withDialect(toPgDialect)
                        .withQuoteAllIdentifiers(false)
                        .withAlwaysUseParentheses(false)
                        .withSelectListItemsOnSeparateLines(false)
                        .withUpdateSetListNewline(false)
                        .withIndentation(0)
                ).getSql();

            CommandSqlType type = CommandSqlType.other;
            if (sqlNode.getKind() == SqlKind.SELECT ||
                sqlNode.getKind() == SqlKind.ORDER_BY ||
                sqlNode.getKind() == SqlKind.WITH)
                type = CommandSqlType.select;
            else if (sqlNode.getKind() == SqlKind.UPDATE)
                type = CommandSqlType.update;
            else if (sqlNode.getKind() == SqlKind.INSERT)
                type = CommandSqlType.insert;
            else if (sqlNode.getKind() == SqlKind.DELETE)
                type = CommandSqlType.delete;
            if (type == CommandSqlType.other) {
                return null;
            }

            return new CommandInfo(type,type == CommandSqlType.select,res, sql);
        } catch (Exception e) {
            if (e instanceof SqlParseException)
                return null;
            else
                throw HdException.convert(e, "mysqlToPgsql");
        }
    }

    private boolean isStringEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    private  CommandInfo parseAntherSql(String sql) {
        try {
            StringReader reader = new StringReader(sql);
            Statement stmt = jsqlParser.parse(reader);

            if (stmt instanceof CreateTable) {
                return getCreateTable((CreateTable)stmt, sql);
            } else if (stmt instanceof  Insert) {
                return getInsert((Insert) stmt);
            } else if (stmt instanceof RollbackStatement) {
                return getRollback((RollbackStatement) stmt, sql);
            } else if (stmt instanceof Commit) {
                return new CommandInfo(CommandSqlType.commit, false, "COMMIT", sql);
            } else if (stmt instanceof CreateIndex) {
                return getCreateIndex((CreateIndex) stmt, sql);
            } else if (stmt instanceof SetStatement) {
                return getSetStatment((SetStatement) stmt, sql);
            } else if (stmt instanceof Drop) {
                return getDrop((Drop) stmt, sql);
            } else if (stmt instanceof UseStatement) {
                return new CommandInfo(CommandSqlType.use, false, sql, sql);
            } else if (stmt instanceof CreateFunction && getScript(sql) != null) {
                return new CommandInfo(CommandSqlType.create_function, false, "SELECT 'CreateFunction' as RESULT", sql);
            } else if (stmt instanceof CreateProcedure && getScript(sql) != null) {
                return new CommandInfo(CommandSqlType.create_procedure, false, "SELECT 'CreateProcedure' as RESULT", sql);
            } else if (stmt instanceof Alter) {
                return getAlterTable((Alter) stmt, sql);
            } else if (stmt instanceof CreateView && getView(sql) != null) {
                return new CommandInfo(CommandSqlType.create_view, false, "SELECT 'CreateView' as RESULT", sql);
            }

            return new CommandInfo(CommandSqlType.other, false, sql, sql);
        } catch (Exception e) {
            String newSql = sql.trim().toUpperCase();
            if (newSql.equals("BEGIN") || newSql.equals("BEGIN:"))
                return new CommandInfo(CommandSqlType.begin, false, "Select 'begin' as result", sql);

            ViewStatmentAttr viewInfo = getView(sql);
            if (viewInfo != null)
                return new CommandInfo(CommandSqlType.create_view, false, "SELECT 'CreateView' as RESULT", sql);

            throw HdException.convert(e, "ParseAntherSql Error sql:" + sql);
        }
    }

    public ScriptStatmentAttr getScript(String sql) {
        try {
            StringReader reader = new StringReader(sql);
            Statement stmt = jsqlParser.parse(reader);
            if (stmt instanceof CreateFunctionalStatement)
            {
                CreateFunctionalStatement funcStmt = (CreateFunctionalStatement) stmt;
                List<String> l = funcStmt.getFunctionDeclarationParts();
                if (l.size() <= 4)
                    return null;
                if (! l.get(1).equals("(") )
                    return null;
                String scriptType = "user_script";
                String name = l.get(0);

                ArrayList<Map.Entry<String, String>> args = new ArrayList<>();

                boolean isOK = false;
                //括号开头要单独处理，结束符偏移量跟后面的参数不一样
                if (l.get(2).equals(")") && l.get(3).equalsIgnoreCase("as")) {
                    isOK = true;
                } else {
                    for (int i = 1; (i + 2) < l.size(); i += 3) {
                        if (l.get(i).equals(")") && l.get(i + 1).equalsIgnoreCase("as")) {
                            isOK = true;
                            break;
                        }
                        args.add(new AbstractMap.SimpleEntry<String, String>(l.get(i + 1), l.get(i + 2)));
                    }
                }

                if (!isOK)
                    return null;
                String content = sql.substring(sql.toLowerCase().replace('\n', ' ').indexOf(" as ") + 4).trim();
                return new ScriptStatmentAttr(
                        funcStmt.getKind().equals("FUNCTION"),
                        false, scriptType, name,args, content);
            }
            return null;
        }catch(Exception e) {
            return null;
        }
    }

    public ViewStatmentAttr getView(String sql) {
        try {
            int asPos = sql.toLowerCase().indexOf(" as");
            if (asPos < 0)
                return null;
            String content = sql.substring(asPos + 4);
            String tmViewSql = sql.substring(0, asPos) + " as select 1 as a";

            StringReader reader = new StringReader(tmViewSql);
            Statement stmt = jsqlParser.parse(reader);
            if (stmt instanceof CreateView)
            {
                CreateView viewStmt = (CreateView) stmt;
                String viewCode = viewStmt.getView().getName();
                boolean isReplace = viewStmt.isOrReplace();
                return new ViewStatmentAttr(
                        isReplace,
                        viewCode, content);
            }
            return null;
        }catch(Exception e) {
            return null;
        }
    }


    private static int countDynamicParam(SqlNode node) {
        if (node instanceof SqlDynamicParam)
            return 1;
        if (! (node instanceof SqlBasicCall))
            return 0;
        SqlBasicCall call = (SqlBasicCall) node;
        int cnt = 0;
        if (call.getOperands() == null)
            return 0;
        for (SqlNode n : call.getOperands()) {
            cnt += countDynamicParam(n);
        }
        return cnt;
    }

    private CommandInfo getExecuteCalcite(SqlNode sqlNode, String originSql) {
        SqlBasicCall call = (SqlBasicCall) sqlNode;
        SqlProcedureCallOperator callOp = (SqlProcedureCallOperator) call.getOperator();
        if(call.getOperands() == null || call.getOperands().length != 1)
            return null;
        SqlBasicCall funCall = (SqlBasicCall) call.getOperands()[0];
        SqlUnresolvedFunction funCallOp = (SqlUnresolvedFunction) funCall.getOperator();
        String funName =  funCallOp.getName().toLowerCase();
        if (!funName.equalsIgnoreCase("query") &&
                !funName.equalsIgnoreCase("run"))
            return null;

        boolean isQuery = funName.equalsIgnoreCase("query");

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT concat('prepare params', ' fake sql'");
        int cnt = countDynamicParam(funCall);
        for (int i = 0; i < cnt; i ++)
            sb.append(",?");
        sb.append(") AS RESULT");

        return new CommandInfo(CommandSqlType.execute, isQuery, sb.toString(), originSql);
    }

    public ArrayList<SqlNode> parseExecuteArgsCalcite(String  sql) {
        try {
            SqlParser sqlParser = SqlParser.create(sql, mysqlConfig);
            SqlNode sqlNode = sqlParser.parseQuery();
            SqlBasicCall call = (SqlBasicCall) sqlNode;
            SqlProcedureCallOperator callOp = (SqlProcedureCallOperator) call.getOperator();
            if(call.getOperands() == null || call.getOperands().length != 1)
                return null;
            SqlBasicCall funCall = (SqlBasicCall) call.getOperands()[0];

            ArrayList<SqlNode> res  = new ArrayList<SqlNode>();

            //todo 还没有递归basecall检查里面，检查是否有：动态参数， String，number,Map,Array等 之外的数据类型
            for (SqlNode node : funCall.getOperands()) {
                if (!(node instanceof SqlLiteral) &&
                    !(node instanceof SqlDynamicParam) &&
                    !(node instanceof SqlBasicCall))
                    throw HdException.get("parseExecuteArgs sql  args error type:" + sql);
                res.add(node);
            }
            return res;

        }catch (Exception e) {
            throw HdException.convert(e, "parseExecuteArgsCalcite error");
        }
    }

    private CommandInfo getAlterTable(Alter stmt, String originSql) {
        if (stmt.getTable()!=null) {
            Table tb = stmt.getTable();
            if (tb.getSchemaName() != null)
                tb.setSchemaName(wrapQuotedString(tb.getSchemaName()));
            if (tb.getName() != null)
                tb.setName(wrapQuotedString(tb.getName()));
        }

        if (stmt.getAlterExpressions() != null) {
            for (AlterExpression aExp : stmt.getAlterExpressions()) {
                if (aExp.getColumnName() != null)
                    aExp.setColumnName(wrapQuotedString(aExp.getColumnName()));
                if (aExp.getColumnOldName() != null)
                    aExp.setColumnOldName(wrapQuotedString(aExp.getColumnOldName()));
                if (aExp.getUkName() != null)
                    aExp.setUkName(wrapQuotedString(aExp.getUkName()));
                if (aExp.getUkColumns() != null) {
                    aExp.setUkColumns(
                            aExp.getUkColumns().stream().map(x -> wrapQuotedString(x)).collect(Collectors.toList()));
                }
                if (aExp.getPkColumns() != null) {
                    aExp.setPkColumns(
                            aExp.getPkColumns().stream().map(x -> wrapQuotedString(x)).collect(Collectors.toList()));
                }
                if (aExp.getFkColumns() != null) {
                    aExp.setFkColumns(
                            aExp.getFkColumns().stream().map(x -> wrapQuotedString(x)).collect(Collectors.toList()));
                }
                if (aExp.getFkSourceSchema() != null)
                    aExp.setFkSourceSchema(wrapQuotedString(aExp.getFkSourceSchema()));
                if (aExp.getFkSourceTable() != null)
                    aExp.setFkSourceTable(wrapQuotedString(aExp.getFkSourceTable()));

                if (aExp.getFkSourceColumns() != null) {
                    aExp.setFkSourceColumns(
                            aExp.getFkSourceColumns().stream().map(x -> wrapQuotedString(x)).collect(Collectors.toList()));
                }
            }
        }

        return new CommandInfo(CommandSqlType.alter, false, stmt.toString(), originSql);
    }

    private CommandInfo getRollback(RollbackStatement stmt, String originSql) {
        RollbackStatement rollbaseStmt = stmt;
        if (isStringEmpty(rollbaseStmt.getForceDistributedTransactionIdentifier())
                && isStringEmpty(rollbaseStmt.getSavepointName())
                && !rollbaseStmt.isUsingSavepointKeyword()
                && !rollbaseStmt.isUsingWorkKeyword()) {
            return new CommandInfo(CommandSqlType.rollback, false, "ROLLBACK", originSql);
        }
        return new CommandInfo(CommandSqlType.other, false, stmt.toString(), originSql);
    }

    private String wrapQuotedString(String oldName) {
        oldName = oldName.trim();
        boolean isQueted = false;
        if (oldName.length() > 2
                && oldName.charAt(0) == '`'
                && oldName.charAt(oldName.length()-1) == '`')
            oldName = oldName.substring(1, oldName.length()-1);
        String newName = "\"" + oldName + "\"";
        return newName;
    }

    //add more commands
    private CommandInfo getCreateTable(CreateTable tbStmt, String originSql) {
        String mediumtextTextType = "varchar (" + (16*1024*1024)+")";
        String longtextTextType = "varchar";
        String tinytextTextType = "varchar(255)";
        if (tbStmt.getColumnDefinitions() != null) {
            for (ColumnDefinition d : tbStmt.getColumnDefinitions()) {
                if (d.getColumnName() != null)
                    d.setColumnName(wrapQuotedString(d.getColumnName()));
                if (d.getColDataType() != null) {
                    boolean isUnsigned = false;
                    if (d.getColumnSpecs() != null &&
                            d.getColumnSpecs().size() >= 1 &&
                            d.getColumnSpecs().get(0).equalsIgnoreCase("unsigned")) {
                        isUnsigned = true;
                        d.setColumnSpecs( d.getColumnSpecs().subList(1, d.getColumnSpecs().size()));
                    }

                    String sType = d.getColDataType().getDataType().toLowerCase();
                    if (sType.equals("mediumtext")) {
                        d.getColDataType().setDataType(mediumtextTextType);
                        //todo json 是否要转?
                    } else if (sType.equals("longtext") || sType.equals("json")) {
                        d.getColDataType().setDataType(longtextTextType);
                    } else if (sType.equals("tinytext")) {
                        d.getColDataType().setDataType(tinytextTextType);
                    } else if (sType.equals("double")
                            || sType.equals("tinyint")
                            || sType.equals("bigint")) {
                        d.getColDataType().setArgumentsStringList(null);
                    } else if (sType.equals("bit")) {
                        d.getColDataType().setDataType("tinyint");
                        d.getColDataType().setArgumentsStringList(null);
                    } else if (sType.equals("enum")) {
                        d.getColDataType().setDataType("varchar");
                        d.getColDataType().setArgumentsStringList(null);
                    } else if (sType.equals("date")) {
                        d.getColDataType().setDataType("timestamp");
                        d.getColDataType().setArgumentsStringList(null);
                    }

                    if (d.getColDataType().getCharacterSet() != null) {
                        d.getColDataType().setCharacterSet(null);
                    }

                    String [] a = new String[]{"a","b"};

                    if (isUnsigned) {
                        String newType = d.getColDataType().getDataType().toLowerCase();
                        if (newType.equals("integer")
                                || newType.equals("int")
                                || newType.equals("int4")
                                || newType.equals("signed")) {
                            d.getColDataType().setDataType("uinteger");
                        } else if (newType.equals("tinyint")
                                ||newType.equals("int1") )  {
                            d.getColDataType().setDataType("utinyint");
                        } else if (newType.equals("smallint")
                                || newType.equals("int2")
                                || newType.equals("short")) {
                            d.getColDataType().setDataType("usmallint");
                        } else if (newType.equals("bigint")
                                || newType.equals("int8")
                                || newType.equals("long")) {
                            d.getColDataType().setDataType("ubigint");
                        }
                        else {
                            throw HdException.get("CreateTable Translator unsigned type error:" + newType);
                        }
                    }
                }
                if (d.getColumnSpecs() != null) {
                    ArrayList<String> sp = new ArrayList<String>();

                    List<String > up = d.getColumnSpecs().stream().map(s -> s.toUpperCase()).collect(Collectors.toList());
                    String sSpec = String.join(" ",up);

                    String[] prefix = new String[] {
                            "PRIMARY KEY",
                            "DEFAULT",
                            "NULL",
                            "NULL DEFAULT",
                            "NOT NULL",
                            "NOT NULL DEFAULT"};
                    int [] expressPos = new int[] { -1, 1, -1, 2, -1, 3};
                    int pos = -1;
                    for (int i = 0; i < prefix.length; i++) {
                        if (sSpec.substring(0,Math.min(prefix[i].length(),sSpec.length())).equals(prefix[i])) {
                            pos = i;
                        }
                    }
                    if (pos != -1) {
                        if (expressPos[pos] != -1 && expressPos[pos] < d.getColumnSpecs().size()) {
                            for (String s :prefix[pos].split(" "))
                                sp.add(s);
                            String expValue = d.getColumnSpecs().get(expressPos[pos]).toLowerCase();
                            // 2 bit data to interger express
                            if (expValue.startsWith("b\'") && expValue.endsWith("\'") && expValue.length() >3) {
                                int parseData = Integer.parseInt(expValue.substring(2, expValue.length()-1), 2);
                                expValue = "\'" + parseData +"\'";
                            }

                            sp.add(expValue);
                            //部分表达公式是函数,例如：uuid()
                            if (d.getColumnSpecs().size() > (expressPos[pos] + 1) &&
                                    d.getColumnSpecs().get(expressPos[pos]+1).equals("()")) {
                                sp.add("()");
                            }
                        }else if (expressPos[pos] == -1) {
                            for (String s :prefix[pos].split(" "))
                                sp.add(s);
                        }
                    }

                    d.setColumnSpecs(sp);
                }
            }
        }

        if (tbStmt.getIndexes()!= null) {
            ArrayList<Index> newIndexes = new ArrayList<>();
            for (Index d : tbStmt.getIndexes()) {
                if (!d.getType().toUpperCase().equals("PRIMARY KEY"))
                    continue;

                if (d.getName() != null)
                    d.setName(wrapQuotedString(d.getName()));
                if (d.getIndexSpec() != null)
                    d.setIndexSpec(new ArrayList<String>());
                if (d.getUsing() != null)
                    d.setUsing(wrapQuotedString(d.getUsing()));
                if (d.getColumnsNames() != null) {
                    ArrayList<String> newNames = new ArrayList<String>();
                    for (String colName : d.getColumnsNames()) {
                        newNames.add(wrapQuotedString(colName));
                    }
                    d.setColumnsNames(newNames);
                }
                newIndexes.add(d);
            }
            tbStmt.setIndexes(newIndexes);
        }

        if (tbStmt.getTable()!=null) {
            Table tb = tbStmt.getTable();
            if (tb.getSchemaName() != null)
                tb.setSchemaName(wrapQuotedString(tb.getSchemaName()));
            if (tb.getName() != null)
                tb.setName(wrapQuotedString(tb.getName()));
        }

        if (tbStmt.getCreateOptionsStrings() != null)
            tbStmt.setCreateOptionsStrings(new ArrayList<String>());
        if (tbStmt.getTableOptionsStrings() != null)
            tbStmt.setTableOptionsStrings(new ArrayList<String>());
        return new CommandInfo(CommandSqlType.create_table, false, tbStmt.toString(), originSql);
    }

    //delete triger
    private CommandInfo getCreateIndex(CreateIndex stmt, String originSql) {
        if (stmt.getTable() != null) {
            Table tb = stmt.getTable();
            if (tb.getSchemaName() != null)
                tb.setSchemaName(wrapQuotedString(tb.getSchemaName()));
            if (tb.getName() != null)
                tb.setName(wrapQuotedString(tb.getName()));
        }
        if (stmt.getIndex() != null) {
            Index d = stmt.getIndex();
            if(d.getName() != null)
                d.setName(wrapQuotedString(d.getName()));
            if (d.getIndexSpec() != null)
                d.setIndexSpec(new ArrayList<String>());
            if (d.getUsing() != null)
                d.setUsing(wrapQuotedString(d.getUsing()));
            if (d.getColumnsNames() != null) {
                ArrayList<String> newNames = new ArrayList<String>();
                for (String colName: d.getColumnsNames()) {
                    newNames.add(wrapQuotedString(colName));
                }
                d.setColumnsNames(newNames);
            }
        }
        return new CommandInfo(CommandSqlType.create_index, false, stmt.toString(), originSql);
    }

    private CommandInfo getSetStatment(SetStatement stmt, String originSql) {
        if (stmt.getName() != null)
            stmt.setName( wrapQuotedString(stmt.getName()));
        return new CommandInfo(CommandSqlType.set, false, stmt.toString(), originSql);
    }

    private CommandInfo getDrop(Drop stmt, String originSql) {
        if (stmt.getName() != null) {
            Table tb = stmt.getName();
            if (tb.getSchemaName() != null)
                tb.setSchemaName( wrapQuotedString(tb.getSchemaName()));
            if (tb.getName() != null)
                tb.setName(wrapQuotedString(tb.getName()));
        }

        return new CommandInfo(CommandSqlType.drop, false, stmt.toString(), originSql);
    }




    public static void flushString(StringBuilder sb, ArrayList<SqlNode> args) {
        if (sb.length() > 0) {
            args.add(SqlLiteral.createCharString(sb.toString(), SqlParserPos.ZERO));
            sb.delete(0, sb.length());
        }
    }

    public static SqlBasicCall createChrFunc(int n) {
        SqlOperator op = new SqlUnresolvedFunction(
                new SqlIdentifier(Arrays.asList(new String[]{"chr"}),SqlParserPos.ZERO),
                null, null, null, null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        ArrayList<SqlNode> args = new ArrayList<SqlNode>();

        args.add(SqlLiteral.createExactNumeric("" + n , SqlParserPos.ZERO));
        SqlBasicCall call = new SqlBasicCall(op, args.toArray(new SqlNode[args.size()]), SqlParserPos.ZERO);
        return call;
    }

    public static SqlNode toConcatSQLExpress(SqlCharStringLiteral literal) {
        String v = literal.getValueAs(String.class);

        SqlOperator op = new SqlUnresolvedFunction(
                new SqlIdentifier(Arrays.asList(new String[]{"concat"}),SqlParserPos.ZERO),
                null, null, null, null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        ArrayList<SqlNode> args = new ArrayList<SqlNode>();

        StringBuilder sb = new StringBuilder();

        boolean preBack = false;
        boolean hasChange = false;
        for (int i  = 0 ; i < v.length(); i++) {
            char c = v.charAt(i);
            if (preBack) {
                switch (c) {
                    case '\'':
                        sb.append("\'\'");
                        break;
                    case '\"':
                        sb.append("\"");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                    case 't':
                        flushString(sb, args);
                        args.add(createChrFunc(9));
//                        sb.append("\'||chr(9)||\'");
                        break;
                    case 'r':
                        flushString(sb, args);
                        args.add(createChrFunc(10));
//                        sb.append("\'||chr(10)||\'");
                        break;
                    case 'n':
                        flushString(sb, args);
                        args.add(createChrFunc(13));
//                        sb.append("\'||chr(13)||\'");
                        break;
                    case 'b':
                        flushString(sb, args);
                        args.add(createChrFunc(8));
//                        sb.append("\'||chr(8)||\'");
                        break;
                    case '0':
                        flushString(sb, args);
                        args.add(createChrFunc(0));
//                        sb.append("\'||chr(0)||\'");
                        break;
                    default:
                        throw HdException.get("TranSQLString Error:" +v +" char:"+c);
                }

                preBack = false;
            } else if (c == '\\') {
                preBack = true;
                hasChange = true;
            } else {
                sb.append(c);
            }
        }
        flushString(sb, args);

        if (preBack)
            throw HdException.get("String has leek backslash:" + v);

        if (hasChange)
            return new SqlBasicCall(op, args.toArray(new SqlNode[args.size()]), SqlParserPos.ZERO);

        return literal;
    }

    String toDuckdbString(String v) {
        StringBuilder sb = new StringBuilder();

        boolean preBack = false;
        for (int i  = 0 ; i < v.length(); i++) {
            char c = v.charAt(i);
            if (preBack) {
                switch (c) {
                    case '\'':
                        sb.append("\'\'");
                        break;
                    case '\"':
                        sb.append("\"");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                    case 't':
                        sb.append("\'||chr(9)||\'");
                        break;
                    case 'r':
                        sb.append("\'||chr(10)||\'");
                        break;
                    case 'n':
                        sb.append("\'||chr(13)||\'");
                        break;
                    case 'b':
                        sb.append("\'||chr(8)||\'");
                        break;
                    case '0':
                        sb.append("\'||chr(0)||\'");
                        break;
                    default:
                        throw HdException.get("TranSQLString Error:" +v +" char:"+c);
                }

                preBack = false;
            } else if (c == '\\') {
                preBack = true;
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }


    String toJdbcString(String v) {
        StringBuilder sb = new StringBuilder();

        boolean preBack = false;
        for (int i  = 0 ; i < v.length(); i++) {
            char c = v.charAt(i);
            if (preBack) {
                switch (c) {
                    case '\'':
                        sb.append('\'');
                        break;
                    case '\"':
                        sb.append('\"');
                        break;
                    case '\\':
                        sb.append('\\');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case '0':
                        sb.append('\0');
                        break;
                    default:
                        throw HdException.get("TranSQLString Error:" +v +" char:"+c);
                }

                preBack = false;
            } else if (c == '\\') {
                preBack = true;
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }


    public String transLoadDataInsertSQL(String sql, ArrayList<String> strList) {
        StringReader reader = new StringReader(sql);
        try {
            Insert stmt = (Insert) jsqlParser.parse(reader);
            if (stmt.getTable() != null) {
                Table tb = stmt.getTable();
                if (tb.getSchemaName() != null)
                    tb.setSchemaName(wrapQuotedString(tb.getSchemaName()));
                if (tb.getName() != null)
                    tb.setName(wrapQuotedString(tb.getName()));
            }

            if (stmt.getColumns() != null) {
                for (Column col : stmt.getColumns()) {
                    col.setColumnName(wrapQuotedString(col.getColumnName()));
                }
            }

            if (stmt.getItemsList() != null) {
                ExpressionList list = (ExpressionList) stmt.getItemsList();

                List<Expression> newItemsList = new ArrayList<Expression>();
                for (Expression exp : list.getExpressions()) {
                    if (exp instanceof StringValue
                            && ((StringValue) exp).getPrefix() != null
                            && ((StringValue) exp).getPrefix().equalsIgnoreCase("B")) {
                        StringValue sExp = (StringValue) exp;
                        int intValue = Integer.parseInt(sExp.getValue(), 2);
                        newItemsList.add(new LongValue("" + intValue));
                    } else if (exp instanceof StringValue) {
                        String sValue = ((StringValue) exp).getValue();
                        strList.add(toJdbcString(sValue));
                        newItemsList.add(new JdbcParameter(strList.size(), false));
                    } else {
                        newItemsList.add(exp);
                    }
                }
                list.setExpressions(newItemsList);
            }

            return stmt.toString();
        }catch (Exception e) {
            throw HdException.convert(e, "transLoadDataInsertSQL error");
        }
    }

    private CommandInfo getInsert(Insert stmt) {
        if (stmt.getTable() != null) {
            Table tb = stmt.getTable();
            if (tb.getSchemaName() != null)
                tb.setSchemaName(wrapQuotedString(tb.getSchemaName()));
            if (tb.getName() != null)
                tb.setName(wrapQuotedString(tb.getName()));
        }

        if (stmt.getColumns() != null) {
            for (Column col : stmt.getColumns()) {
                col.setColumnName(wrapQuotedString(col.getColumnName()) );
            }
        }

        if (stmt.getItemsList() != null) {
            ExpressionList list = (ExpressionList)stmt.getItemsList();

            List<Expression> newItemsList = new ArrayList<Expression>();
            for (Expression exp:list.getExpressions()) {
                if (exp instanceof StringValue
                        &&  ( (StringValue) exp).getPrefix() != null
                        && ( (StringValue) exp).getPrefix().equalsIgnoreCase("B")) {
                    StringValue sExp = (StringValue) exp;
                    int intValue = Integer.parseInt(sExp.getValue(), 2);
                    newItemsList.add(new LongValue("" + intValue));
                }else if(exp instanceof StringValue) {
                    String sValue = ((StringValue) exp).getValue();
                    newItemsList.add(new StringValue(toDuckdbString(sValue)));
                }else {
                    newItemsList.add(exp);
                }
            }
            list.setExpressions(newItemsList);
        }

        return new CommandInfo(CommandSqlType.insert, false, stmt.toString());
    }
}
