package testhdserver.jdbc;

import org.duckdb.DuckDBConnection;
import testhdserver.TestBase;
import testhdserver.TestDb;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class TestMysqlExtension extends TestDb {

    public void testQuery() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");


        Properties ro_prop = new Properties();
        ro_prop.setProperty("duckdb.read_only", "true");
//        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:/Users/kevin/h2duck_test_temp/test_copy", ro_prop);

        Statement stmt = conn.createStatement();

        //mysql agent
        stmt.execute("INSTALL mysql");
        stmt.execute("load mysql");
        stmt.execute("ATTACH 'host=192.168.1.1 user=oryx_test port=13306 password=youxin@2023 database=oryx_order' AS mysqldb (TYPE mysql)");
        stmt.execute("use mysqldb");

//        conn.setAutoCommit(false);
        for (int i = 0; i < 10; i ++) {
            long tPreBegin = System.currentTimeMillis();
            PreparedStatement pStmt = conn.prepareStatement("SELECT count(*) FROM oryx_order.order_info");
            long tPreEnd = System.currentTimeMillis();
            ResultSet r = pStmt.executeQuery();

//        ResultSet r = stmt.executeQuery("SELECT count(*) FROM oryx_order.oryx_order.order_info");
            long tExeEnd = System.currentTimeMillis();

            System.out.println("1 p:" + (tPreEnd - tPreBegin));
            System.out.println("1 r:" + (tExeEnd - tPreEnd));


            long tPreBegin2 = System.currentTimeMillis();
            PreparedStatement pStmt2 = conn.prepareStatement("SELECT * FROM oryx_goods.goods");
            long tPreEnd2 = System.currentTimeMillis();
            ResultSet r2 = pStmt2.executeQuery();

//        ResultSet r2 = stmt.executeQuery("SELECT count(*) FROM oryx_order.oryx_order.order_info");

            long tExeEnd2 = System.currentTimeMillis();

            System.out.println("2 p:" + (tPreEnd2 - tPreBegin2));
            System.out.println("2 r:" + (tExeEnd2 - tPreEnd2));
            System.out.println("---");

        }
//        conn.commit();
//        conn.setAutoCommit(true);
    }

    public void testCopyDatabase() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:/Users/kevin/h2duck_test_temp/test_copy");
        Statement stmt = conn.createStatement();

        stmt.execute("INSTALL mysql");
        stmt.execute("load mysql");
//                stmt.execute("ATTACH 'test_copy' AS mysqldb");

        stmt.execute("ATTACH 'host=192.168.1.1 user=oryx_test port=13306 password=youxin@2023 database=oryx_order' AS mysqldb (TYPE mysql)");
        long tStart = System.currentTimeMillis();
        System.out.println("start copy:");
        stmt.execute("COPY FROM DATABASE mysqldb to test_copy");
        System.out.println("start export:");
        long tCopy = System.currentTimeMillis();
        stmt.execute("EXPORT DATABASE '/Users/kevin/h2duck_test_temp_checkpoint' (FORMAT PARQUET)");
        long tExport = System.currentTimeMillis();
        System.out.println("copy:" + (tCopy-tStart));
        System.out.println("export:" + (tExport-tCopy));
    }


    @Override
    public void test() throws Exception {
        testQuery();
//        testCopyDatabase();
    }
}
