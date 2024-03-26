package hdserver;

import org.yaml.snakeyaml.Yaml;
import java.io.FileReader;


public class DBConfig {
    private static DBConfig gInstance = null;

    public int port = 4007;
    public String engineType = "duckdb";
    public int maxConnection = 50;

    public int maxSqlTranCacheSize = 10000;
    public int sqlTimeout = 30;
    public int logFileRowLimit = 1000000;
    public int logFileCount = 10;

    public String mysqlIp = "";
    public int mysqlPort = 3306;
    public String mysqlUser = "root";
    public String mysqlPassword = "123";
    public int mysqlSchemaCacheTime = 600;
    public boolean forbidWrite = false;
    public String name;
    public String baseDir;
    public String logDir;


    public static boolean load(String path) {
        if (gInstance != null)
            return false;
        try {
            Yaml yaml =  new Yaml();
            FileReader fr = new FileReader(path);
            gInstance = yaml.loadAs(fr, DBConfig.class);
        }catch (Exception e) {
            System.out.println("DBConfig load Error:" + e.getMessage());
            return false;
        }

        return true;
    }

    public static DBConfig getInstance() {
        return gInstance;
    }
}
