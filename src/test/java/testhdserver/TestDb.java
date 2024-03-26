/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package testhdserver;

import hdserver.DBConfig;
import hdserver.TcpServer;
import org.h2.store.fs.FileUtils;
import org.h2.tools.Server;
import testhdserver.utils.SelfDestructor;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The base class for tests that use connections to database.
 */
public abstract class TestDb extends TestBase {

    /**
     * Open a database connection in admin mode. The default user name and
     * password is used.
     *
     * @param name the database name
     * @return the connection
     */
    public Connection getConnection(String name) throws SQLException {
        return getConnectionInternal_ex(name);
//        return getConnectionInternal(getURL(name, true), getUser(),
//                getPassword());
    }

    public Server createServer() throws SQLException {
        TcpServer tcpServer = new TcpServer();
        int port = DBConfig.getInstance().port;
        Server server = new Server(tcpServer, new String[] {"-tcpPort", ""+port,"-ifNotExists"});
        tcpServer.setShutdownHandler(server);
        server.start();
        return server;
    }




    /**
     * Open a database connection.
     *
     * @param name the database name
     * @param user the user name to use
     * @param password the password to use
     * @return the connection
     */
    public Connection getConnection(String name, String user, String password)
            throws SQLException {
        return getConnectionInternal_ex(name);

//        return getConnectionInternal(getURL(name, false), user, password);
    }

    /**
     * Get the database URL for the given database name using the current
     * configuration options.
     *
     * @param name the database name
     * @param admin true if the current user is an admin
     * @return the database URL
     */
    protected String getURL(String name, boolean admin) {
        String url;
        if (name.startsWith("jdbc:")) {
            name = addOption(name, "MV_STORE", "true");
            return name;
        }
        if (admin) {
            // name = addOption(name, "RETENTION_TIME", "10");
            // name = addOption(name, "WRITE_DELAY", "10");
        }
        int idx = name.indexOf(':');
        if (idx == -1 && config.memory) {
            name = "mem:" + name;
        } else {
            if (idx < 0 || idx > 10) {
                // index > 10 if in options
                name = getBaseDir() + "/" + name;
            }
        }
        if (config.networked) {
            if (config.ssl) {
                url = "ssl://localhost:"+config.getPort()+"/" + name;
            } else {
                url = "tcp://localhost:"+config.getPort()+"/" + name;
            }
        } else if (config.googleAppEngine) {
            url = "gae://" + name +
                    ";FILE_LOCK=NO;AUTO_SERVER=FALSE;DB_CLOSE_ON_EXIT=FALSE";
        } else {
            url = name;
        }
        url = addOption(url, "MV_STORE", "true");
        url = addOption(url, "MAX_COMPACT_TIME", "0"); // to speed up tests
        if (!config.memory) {
            if (config.smallLog && admin) {
                url = addOption(url, "MAX_LOG_SIZE", "1");
            }
        }
        if (config.traceSystemOut) {
            url = addOption(url, "TRACE_LEVEL_SYSTEM_OUT", "2");
        }
        if (config.traceLevelFile > 0 && admin) {
            url = addOption(url, "TRACE_LEVEL_FILE", "" + config.traceLevelFile);
            url = addOption(url, "TRACE_MAX_FILE_SIZE", "8");
        }
        if (config.throttleDefault > 0) {
            url = addOption(url, "THROTTLE", "" + config.throttleDefault);
        } else if (config.throttle > 0) {
            url = addOption(url, "THROTTLE", "" + config.throttle);
        }
        url = addOption(url, "LOCK_TIMEOUT", "" + config.lockTimeout);
        if (config.diskUndo && admin) {
            url = addOption(url, "MAX_MEMORY_UNDO", "3");
        }
        if (config.big && admin) {
            // force operations to disk
            url = addOption(url, "MAX_OPERATION_MEMORY", "1");
        }
        if (config.lazy) {
            url = addOption(url, "LAZY_QUERY_EXECUTION", "1");
        }
        if (config.cacheType != null && admin) {
            url = addOption(url, "CACHE_TYPE", config.cacheType);
        }
        if (config.diskResult && admin) {
            url = addOption(url, "MAX_MEMORY_ROWS", "100");
            url = addOption(url, "CACHE_SIZE", "0");
        }
        if (config.cipher != null) {
            url = addOption(url, "CIPHER", config.cipher);
        }
        if (config.collation != null) {
            url = addOption(url, "COLLATION", config.collation);
        }
        return "jdbc:h2:" + url;
    }

    private static String addOption(String url, String option, String value) {
        if (url.indexOf(";" + option + "=") < 0) {
            url += ";" + option + "=" + value;
        }
        return url;
    }

    public static String getMyDir() {
        return DBConfig.getInstance().baseDir;
//        return "/Users/kevin/hdservertemp";
    }

    protected void deleteDb(String name) {
        deleteDb(
                DBConfig.getInstance().baseDir
                ,DBConfig.getInstance().name
        );
    }

    /**
     * Delete all database files for a database.
     *
     * @param dir the directory where the database files are located
     * @param name the database name
     */
    protected void deleteDb(String dir, String name) {
        String filename = FileUtils.toRealPath(dir + "/" + name);
        System.out.println("try delete db :" + filename);
        File dbDir = new File(dir);

        for (File f :dbDir.listFiles()) {
            String path = f.getAbsolutePath();
            if (path.equals(filename) ||
                    path.equals(filename+".wal") ||
                    path.equals(filename+".tmp") ||
                    path.startsWith(filename + "_p_meta_") ||
                    path.startsWith(filename + "_p_data_") ||
                    path.startsWith(filename + "_w_")) {
                System.out.println("delete db file:" + path);
                FileUtils.tryDelete(path);
            }
        }

    }

    private static boolean deleteFile(File file) {
        if (file == null || !file.exists())
            return false;
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                deleteFile(f);
            }else {
                f.delete();
                System.out.println("delete:" + f.getName());
            }
        }
        file.delete();
        System.out.println("delete:" + file.getName());
        return true;
    }

    private static Connection getConnectionInternal_ex(String name) throws SQLException {
        org.h2.Driver.load();
        int port = DBConfig.getInstance().port;
        String url = "jdbc:h2:tcp://localhost:" + port +"/" + getMyDir() + "/" + name + ";MODE=MySQL";
        return DriverManager.getConnection(url);
    }

    /**
     * Build a child process.
     *
     * @param name the name
     * @param childClass the class
     * @param jvmArgs the argument list
     * @return the process builder
     */
    public ProcessBuilder buildChild(String name, Class<? extends TestDb> childClass,
                                     String... jvmArgs) {
        List<String> args = new ArrayList<>(16);
        args.add(getJVM());
        Collections.addAll(args, jvmArgs);
        Collections.addAll(args, "-cp", getClassPath(),
                SelfDestructor.getPropertyString(1),
                childClass.getName(),
                "-url", getURL(name, true),
                "-user", getUser(),
                "-password", getPassword());
        ProcessBuilder processBuilder = new ProcessBuilder()
//                            .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .command(args);
        return processBuilder;
    }

    public abstract static class Child extends TestDb {
        private String url;
        private String user;
        private String password;

        public Child(String... args) {
            for (int i = 0; i < args.length; i++) {
                if ("-url".equals(args[i])) {
                    url = args[++i];
                } else if ("-user".equals(args[i])) {
                    user = args[++i];
                } else if ("-password".equals(args[i])) {
                    password = args[++i];
                }
                SelfDestructor.startCountdown(60);
            }
        }

        public Connection getConnection() throws SQLException {
            return getConnection(url, user, password);
        }
    }

}

