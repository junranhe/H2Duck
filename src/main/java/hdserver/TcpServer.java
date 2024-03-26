/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package hdserver;

import org.duckdb.DuckDBConnection;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.server.ShutdownHandler;
import org.h2.util.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TcpServer implements org.h2.server.Service {

    private static final int SHUTDOWN_NORMAL = 0;
    private static final int SHUTDOWN_FORCE = 1;
    private static final String MANAGEMENT_DB_PREFIX = "management_db_";
    private static final ConcurrentHashMap<Integer, TcpServer> SERVERS = new ConcurrentHashMap<>();

    private int port;
    private boolean portIsSet;
    private boolean trace;
    private boolean ssl;
    private boolean stop;
    private ShutdownHandler shutdownHandler;
    private ServerSocket serverSocket;
    private final Set<TcpServerThread> running =
            Collections.synchronizedSet(new HashSet<TcpServerThread>());
    private String baseDir;
    private boolean allowOthers;
    private boolean isDaemon;
    private boolean ifExists = true;
    private Connection managementDb;
    private PreparedStatement managementDbAdd;
    private PreparedStatement managementDbRemove;
    private String managementPassword = "";
    private Thread listenerThread;
    private int nextThreadId;
    private String key, keyDatabase;
    private Timer sessionTimeoutChecker;
    private Timer mysqlCacheClearTask;

    public static class CheckTimeoutTask extends TimerTask {
        private Set<TcpServerThread> running;
        public CheckTimeoutTask(Set<TcpServerThread> _running) {
            running = _running;
        }
        public void run() {
            for (TcpServerThread oneThread : new ArrayList<>(running)) {
                    oneThread.checkAndCancelTimeoutTask();
            }
        }
    }

    /**
     * Get the database name of the management database.
     * The management database contains a table with active sessions (SESSIONS).
     *
     * @param port the TCP server port
     * @return the database name (usually starting with mem:)
     */
    public static String getManagementDbName(int port) {
        return "mem:" + MANAGEMENT_DB_PREFIX + port;
    }

    private void initManagementDb() throws SQLException {
        if (managementPassword.isEmpty()) {
            managementPassword = StringUtils.convertBytesToHex(MathUtils.secureRandomBytes(32));
        }
        // avoid using the driver manager
        Connection conn=null;
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        } catch (Exception e) {
            throw DbException.get(ErrorCode.GENERAL_ERROR_1, "initMangementDb error");
        }
        managementDb = conn;

        try (Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE IF NOT EXISTS SESSIONS" +
                    "(ID INT PRIMARY KEY, URL VARCHAR, \"USER\" VARCHAR, " +
                    "CONNECTED INT)");
            managementDbAdd = conn.prepareStatement(
                    "INSERT INTO SESSIONS VALUES(?, ?, ?, 1)");
            managementDbRemove = conn.prepareStatement(
                    "DELETE FROM SESSIONS WHERE ID=?");
        }
        SERVERS.put(port, this);
    }

    /**
     * Shut down this server.
     */
    void shutdown() {
        if (shutdownHandler != null) {
            shutdownHandler.shutdown();
        }
    }

    public void setShutdownHandler(ShutdownHandler shutdownHandler) {
        this.shutdownHandler = shutdownHandler;
    }

    /**
     * Add a connection to the management database.
     *
     * @param id the connection id
     * @param url the database URL
     * @param user the user name
     */
    synchronized void addConnection(int id, String url, String user) {
        try {
            managementDbAdd.setInt(1, id);
            managementDbAdd.setString(2, url);
            managementDbAdd.setString(3, user);
            managementDbAdd.execute();
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
    }

    /**
     * Remove a connection from the management database.
     *
     * @param id the connection id
     */
    synchronized void removeConnection(int id) {
        try {
            managementDbRemove.setInt(1, id);
            managementDbRemove.execute();
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
    }

    private synchronized void stopManagementDb() {
        if (managementDb != null) {
            try {
                managementDb.close();
            } catch (SQLException e) {
                DbException.traceThrowable(e);
            }
            managementDb = null;
        }
    }

    @Override
    public void init(String... args) {
        port = Constants.DEFAULT_TCP_PORT;
        for (int i = 0; args != null && i < args.length; i++) {
            String a = args[i];
            if (Tool.isOption(a, "-trace")) {
                trace = true;
            } else if (Tool.isOption(a, "-tcpSSL")) {
                ssl = true;
            } else if (Tool.isOption(a, "-tcpPort")) {
                port = Integer.decode(args[++i]);
                portIsSet = true;
            } else if (Tool.isOption(a, "-tcpPassword")) {
                managementPassword = args[++i];
            } else if (Tool.isOption(a, "-baseDir")) {
                baseDir = args[++i];
            } else if (Tool.isOption(a, "-key")) {
                key = args[++i];
                keyDatabase = args[++i];
            } else if (Tool.isOption(a, "-tcpAllowOthers")) {
                allowOthers = true;
            } else if (Tool.isOption(a, "-tcpDaemon")) {
                isDaemon = true;
            } else if (Tool.isOption(a, "-ifExists")) {
                ifExists = true;
            } else if (Tool.isOption(a, "-ifNotExists")) {
                ifExists = false;
            }
        }
    }

    @Override
    public String getURL() {
        return (ssl ? "ssl" : "tcp") + "://" + NetUtils.getLocalAddress() + ":" + port;
    }

    @Override
    public int getPort() {
        return port;
    }

    /**
     * Returns whether a secure protocol is used.
     *
     * @return {@code true} if SSL socket is used, {@code false} if plain socket
     *         is used
     */
    public boolean getSSL() {
        return ssl;
    }

    /**
     * Check if this socket may connect to this server. Remote connections are
     * not allowed if the flag allowOthers is set.
     *
     * @param socket the socket
     * @return true if this client may connect
     */
    boolean allow(Socket socket) {
        if (allowOthers) {
            return true;
        }
        try {
            return NetUtils.isLocalAddress(socket);
        } catch (UnknownHostException e) {
            traceError(e);
            return false;
        }
    }

    @Override
    public synchronized void start() throws SQLException {
        stop = false;
        try {
            serverSocket = NetUtils.createServerSocket(port, ssl);
        } catch (DbException e) {
            if (!portIsSet) {
                serverSocket = NetUtils.createServerSocket(0, ssl);
            } else {
                throw e;
            }
        }
        port = serverSocket.getLocalPort();

        initManagementDb();
        sessionTimeoutChecker = new Timer();
        sessionTimeoutChecker.scheduleAtFixedRate(
                new CheckTimeoutTask(running), 1000, 1000);

    }

    @Override
    public void listen() {
        listenerThread = Thread.currentThread();
        String threadName = listenerThread.getName();

        try {
            long preTime = System.currentTimeMillis();
            while (!stop) {
                Socket s = serverSocket.accept();

                //connection limit
                if (running.size() >= DBConfig.getInstance().maxConnection) {
                    s.close();
                    Log.warning("connection limit:" + running.size() +
                            "/" + DBConfig.getInstance().maxConnection);
                    continue;
                }

                Utils10.setTcpQuickack(s, true);
                int id = nextThreadId++;
                TcpServerThread c = new TcpServerThread(s, this, id);
                running.add(c);
                Thread thread = new Thread(c, threadName + " thread-" + id);
                thread.setDaemon(isDaemon);
                c.setThread(thread);
                thread.start();
                Log.info("current connection:" + running.size() + "/" + DBConfig.getInstance().maxConnection);
            }
            serverSocket = NetUtils.closeSilently(serverSocket);
        } catch (Exception e) {
            if (!stop) {
                DbException.traceThrowable(e);
            }
        }
        stopManagementDb();
        sessionTimeoutChecker.cancel();
    }

    @Override
    public synchronized boolean isRunning(boolean traceError) {
        if (serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        } catch (Exception e) {
            if (traceError) {
                traceError(e);
            }
            return false;
        }
    }

    @Override
    public void stop() {
        // TODO server: share code between web and tcp servers
        // need to remove the server first, otherwise the connection is broken
        // while the server is still registered in this map
        SERVERS.remove(port);
        if (!stop) {
            stopManagementDb();
            sessionTimeoutChecker.cancel();
            stop = true;
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    DbException.traceThrowable(e);
                } catch (NullPointerException e) {
                    // ignore
                }
                serverSocket = null;
            }
            if (listenerThread != null) {
                try {
                    listenerThread.join(1000);
                } catch (InterruptedException e) {
                    DbException.traceThrowable(e);
                }
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        for (TcpServerThread c : new ArrayList<>(running)) {
            if (c != null) {
                c.close();
                try {
                    c.getThread().join(100);
                } catch (Exception e) {
                    DbException.traceThrowable(e);
                }
            }
        }

    }

    /**
     * Stop a running server. This method is called via reflection from the
     * STOP_SERVER function.
     *
     * @param port the port where the server runs, or 0 for all running servers
     * @param password the password (or null)
     * @param shutdownMode the shutdown mode, SHUTDOWN_NORMAL or SHUTDOWN_FORCE.
     */
    public static void stopServer(int port, String password, int shutdownMode) {
        if (port == 0) {
            for (int p : SERVERS.keySet().toArray(new Integer[0])) {
                if (p != 0) {
                    stopServer(p, password, shutdownMode);
                }
            }
            return;
        }
        TcpServer server = SERVERS.get(port);
        if (server == null) {
            return;
        }
        if (!server.managementPassword.equals(password)) {
            return;
        }
        if (shutdownMode == SHUTDOWN_NORMAL) {
            server.stopManagementDb();

            server.stop = true;
            try {
                Socket s = NetUtils.createLoopbackSocket(port, false);
                s.close();
            } catch (Exception e) {
                // try to connect - so that accept returns
            }
        } else if (shutdownMode == SHUTDOWN_FORCE) {
            server.stop();
        }
        server.shutdown();
    }

    /**
     * Remove a thread from the list.
     *
     * @param t the thread to remove
     */
    void remove(TcpServerThread t) {
        running.remove(t);
    }

    /**
     * Get the configured base directory.
     *
     * @return the base directory
     */
    String getBaseDir() {
        return baseDir;
    }

    /**
     * Print a message if the trace flag is enabled.
     *
     * @param s the message
     */
    void trace(String s) {
        if (trace) {
            Log.info(s);
        }
    }
    /**
     * Print a stack trace if the trace flag is enabled.
     *
     * @param e the exception
     */
    void traceError(Throwable e) {
        if (trace) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean getAllowOthers() {
        return allowOthers;
    }

    @Override
    public String getType() {
        return "TCP";
    }

    @Override
    public String getName() {
        return "H2 TCP Server";
    }

    boolean getIfExists() {
        return ifExists;
    }

    /**
     * Stop the TCP server with the given URL.
     *
     * @param url the database URL
     * @param password the password
     * @param force if the server should be stopped immediately
     * @param all whether all TCP servers that are running in the JVM should be
     *            stopped
     * @throws SQLException on failure
     */
    public static synchronized void shutdown(String url, String password,
                                             boolean force, boolean all) throws SQLException {
        //todo 需要补充权限控制，避免随意可以shutdown服务器
        try {
            int port = Constants.DEFAULT_TCP_PORT;
            int idx = url.lastIndexOf(':');
            if (idx >= 0) {
                String p = url.substring(idx + 1);
                if (StringUtils.isNumber(p)) {
                    port = Integer.decode(p);
                }
            }
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Cancel a running statement.
     *
     * @param sessionId the session id
     * @param statementId the statement id
     */
    void cancelStatement(String sessionId, int statementId) {
        //todo 是否需要移走？或者需要补充处理是否cancel了对应的语句
        for (TcpServerThread c : new ArrayList<>(running)) {
            if (c != null) {
                c.cancelStatement(sessionId, statementId);
            }
        }
    }

    /**
     * If no key is set, return the original database name. If a key is set,
     * check if the key matches. If yes, return the correct database name. If
     * not, throw an exception.
     *
     * @param db the key to test (or database name if no key is used)
     * @return the database name
     * @throws DbException if a key is set but doesn't match
     */
    public String checkKeyAndGetDatabaseName(String db) {
        if (key == null) {
            return db;
        }
        if (key.equals(db)) {
            return keyDatabase;
        }
        throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
    }

    @Override
    public boolean isDaemon() {
        return isDaemon;
    }

}
