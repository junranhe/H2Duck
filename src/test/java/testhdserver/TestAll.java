package testhdserver;

/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */


import hdserver.DBConfig;
import hdserver.Log;
import org.h2.Driver;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.*;
import testhdserver.db.TestTransaction;
import testhdserver.jdbc.*;
import testhdserver.utils.OutputCatcher;
import testhdserver.utils.SelfDestructor;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * The main test application. JUnit is not used because loops are easier to
 * write in regular java applications (most tests are ran multiple times using
 * different settings).
 */
public class TestAll {

    static {
        // Locale.setDefault(new Locale("ru", "ru"));
    }

/*

PIT test:
java org.pitest.mutationtest.MutationCoverageReport
--reportDir data --targetClasses org.h2.dev.store.btree.StreamStore*
--targetTests org.h2.test.store.TestStreamStore
--sourceDirs src/test,src/tools

Dump heap on out of memory:
-XX:+HeapDumpOnOutOfMemoryError

Random test:
java15
cd h2database/h2/bin
del *.db
start cmd /k "java -cp .;%H2DRIVERS% org.h2.test.TestAll join >testJoin.txt"
start cmd /k "java -cp . org.h2.test.TestAll synth >testSynth.txt"
start cmd /k "java -cp . org.h2.test.TestAll all >testAll.txt"
start cmd /k "java -cp . org.h2.test.TestAll random >testRandom.txt"
start cmd /k "java -cp . org.h2.test.TestAll btree >testBtree.txt"
start cmd /k "java -cp . org.h2.test.TestAll halt >testHalt.txt"
java -cp . org.h2.test.TestAll crash >testCrash.txt

java org.h2.test.TestAll timer

*/

    /**
     * Set to true if any of the tests fail. Used to return an error code from
     * the whole program.
     */
    static boolean atLeastOneTestFailed;

    /**
     * If the test should run with many rows.
     */
    public boolean big;

    /**
     * If remote database connections should be used.
     */
    public boolean networked;

    /**
     * If in-memory databases should be used.
     */
    public boolean memory;

    /**
     * If code coverage is enabled.
     */
    public boolean codeCoverage;

    /**
     * If lazy queries should be used.
     */
    public boolean lazy;

    /**
     * The cipher to use (null for unencrypted).
     */
    public String cipher;

    /**
     * The file trace level value to use.
     */
    public int traceLevelFile;

    /**
     * If test trace information should be written (for debugging only).
     */
    public boolean traceTest;

    /**
     * If testing on Google App Engine.
     */
    public boolean googleAppEngine;

    /**
     * If a small cache and a low number for MAX_MEMORY_ROWS should be used.
     */
    public boolean diskResult;

    /**
     * Test using the recording file system.
     */
    public boolean reopen;

    /**
     * Test the split file system.
     */
    public boolean splitFileSystem;

    /**
     * If only fast CI tests should be run.
     */
    public boolean ci;

    /**
     * the vmlens.com race condition tool
     */
    public boolean vmlens;

    /**
     * The lock timeout to use
     */
    public int lockTimeout = 50;

    /**
     * If the transaction log should be kept small (that is, the log should be
     * switched early).
     */
    boolean smallLog;

    /**
     * If SSL should be used for remote connections.
     */
    boolean ssl;

    /**
     * If MAX_MEMORY_UNDO=3 should be used.
     */
    boolean diskUndo;

    /**
     * If TRACE_LEVEL_SYSTEM_OUT should be set to 2 (for debugging only).
     */
    boolean traceSystemOut;

    /**
     * If the tests should run forever.
     */
    boolean endless;

    /**
     * The THROTTLE value to use.
     */
    public int throttle;

    /**
     * The THROTTLE value to use by default.
     */
    int throttleDefault = Integer.parseInt(System.getProperty("throttle", "0"));

    /**
     * If the test should stop when the first error occurs.
     */
    boolean stopOnError;

    /**
     * The cache type.
     */
    String cacheType;

    /** If not null the database should be opened with the collation parameter */
    public String collation;


    /**
     * The AB-BA locking detector.
     */
    AbbaLockingDetector abbaLockingDetector;

    /**
     * The list of tests.
     */
    ArrayList<TestBase> tests = new ArrayList<>();

    private Server server;

    HashSet<String> excludedTests = new HashSet<>();

    /**
     * The map of executed tests to detect not executed tests.
     * Boolean value is 'false' for a disabled test.
     */
    HashMap<Class<? extends TestBase>, Boolean> executedTests = new HashMap<>();

    /**
     * Run all tests.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        String cfg_path = args[0];
//        System.out.println("cfg_path:" + cfg_path);
//        DBConfig.load("/Users/kevin/Documents/code/opensource/H2Duck/src/main/resources/testconfig.yaml");
        DBConfig.load(cfg_path);
        Log.init(DBConfig.getInstance().logDir + "/test.log",
                DBConfig.getInstance().logFileRowLimit,
                DBConfig.getInstance().logFileCount);

        OutputCatcher catcher = OutputCatcher.start();
        run(new String[]{});
        catcher.stop();
        catcher.writeTo("Test Output", "docs/html/testOutput.html");
        if (atLeastOneTestFailed) {
            System.exit(1);
        }
    }

    private static void run(String... args) throws Exception {
        SelfDestructor.startCountdown(4 * 60);
        long time = System.nanoTime();
        printSystemInfo();

        // use lower values, to better test those cases,
        // and (for delays) to speed up the tests

        System.setProperty("h2.maxMemoryRows", "100");

        System.setProperty("h2.delayWrongPasswordMin", "0");
        System.setProperty("h2.delayWrongPasswordMax", "0");
        System.setProperty("h2.useThreadContextClassLoader", "true");

        // System.setProperty("h2.modifyOnWrite", "true");

        // speedup
        // System.setProperty("h2.syncMethod", "");

/*

recovery tests with small freeList pages, page size 64

reopen org.h2.test.unit.TestPageStore
-Xmx1500m -D reopenOffset=3 -D reopenShift=1

power failure test
power failure test: larger binaries and additional index.
power failure test with randomly generating / dropping indexes and tables.

drop table test;
create table test(id identity, name varchar(100) default space(100));
@LOOP 10 insert into test select null, null from system_range(1, 100000);
delete from test;

documentation: review package and class level javadocs
documentation: rolling review at main.html

-------------

kill a test:
kill -9 `jps -l | grep "org.h2.test." | cut -d " " -f 1`

*/
        TestAll test = new TestAll();
        if (args.length > 0) {
//            if ("ci".equals(args[0])) {
//                test.ci = true;
//                test.testAll(args, 1);
//            } else if ("vmlens".equals(args[0])) {
//                test.vmlens = true;
//                test.testAll(args, 1);
//            } else if ("reopen".equals(args[0])) {
//                System.setProperty("h2.delayWrongPasswordMin", "0");
//                System.setProperty("h2.analyzeAuto", "100");
//                System.setProperty("h2.pageSize", "64");
//                System.setProperty("h2.reopenShift", "5");
//                FilePathRec.register();
//                test.reopen = true;
//                TestReopen reopen = new TestReopen();
//                reopen.init();
//                FilePathRec.setRecorder(reopen);
//                test.runTests();
//            } else if ("crash".equals(args[0])) {
//                test.endless = true;
//                new TestCrashAPI().runTest(test);
//            } else if ("synth".equals(args[0])) {
//                new TestSynth().runTest(test);
//            } else if ("kill".equals(args[0])) {
//                new TestKill().runTest(test);
//            } else if ("random".equals(args[0])) {
//                test.endless = true;
//                new TestRandomSQL().runTest(test);
//            } else if ("join".equals(args[0])) {
//                new TestJoin().runTest(test);
//                test.endless = true;
//            } else if ("btree".equals(args[0])) {
//                new TestBtreeIndex().runTest(test);
//            } else if ("all".equals(args[0])) {
//                test.testEverything();
//            } else if ("codeCoverage".equals(args[0])) {
//                test.codeCoverage = true;
//                test.runCoverage();
//            } else if ("multiThread".equals(args[0])) {
//                new TestMulti().runTest(test);
//            } else if ("halt".equals(args[0])) {
//                new TestHaltApp().runTest(test);
//            } else if ("timer".equals(args[0])) {
//                new TestTimer().runTest(test);
//            }
        } else {
            test.testAll(args, 0);
        }
        System.out.println(TestBase.formatTime(new StringBuilder(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time)).append(" total").toString());
    }

    private void testAll(String[] args, int offset) throws Exception {
        int l = args.length;
        while (l > offset + 1) {
            if ("-exclude".equals(args[offset])) {
                excludedTests.add(args[offset + 1]);
                offset += 2;
            } else {
                break;
            }
        }
        runTests();
//        if (!ci && !vmlens) {
//            Profiler prof = new Profiler();
//            prof.depth = 16;
//            prof.interval = 1;
//            prof.startCollecting();
//            TestPerformance.main("-init", "-db", "1", "-size", "1000");
//            prof.stopCollecting();
//            System.out.println(prof.getTop(5));
//            TestPerformance.main("-init", "-db", "1", "-size", "1000");
//        }
    }

    /**
     * Run all tests in all possible combinations.
     */
    private void testEverything() throws SQLException {
        for (int c = 0; c < 2; c++) {
            if (c == 0) {
                cipher = null;
            } else {
                cipher = "AES";
            }
            for (int a = 0; a < 64; a++) {
                smallLog = (a & 1) != 0;
                big = (a & 2) != 0;
                networked = (a & 4) != 0;
                memory = (a & 8) != 0;
                ssl = (a & 16) != 0;
                diskResult = (a & 32) != 0;
                for (int trace = 0; trace < 3; trace++) {
                    traceLevelFile = trace;
                    test();
                }
            }
        }
    }

    /**
     * Run the tests with a number of different settings.
     */
    private void runTests() throws SQLException {

        if (Boolean.getBoolean("abba")) {
            abbaLockingDetector = new AbbaLockingDetector().startCollecting();
        }

        smallLog = big = networked = memory = lazy = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        traceTest = stopOnError = false;
        traceLevelFile = throttle = 0;
        cipher = null;

        // memory is a good match for multi-threaded, makes things happen
        // faster, more chance of exposing race conditions
//        memory = true;
//        test();
//        if (vmlens) {
//            return;
//        }
//        testAdditional();

        // test utilities
//        big = !ci;
//        testUtils();
//        big = false;

        // lazy
//        lazy = true;
//        memory = true;
//        test();
//        lazy = false;
//
//        // but sometimes race conditions need bigger windows
//        memory = false;
//        test();
//        testAdditional();

        networked = true;

        memory = true;
        test();
//        memory = false;
//
//        lazy = true;
//        test();
//        lazy = false;
//
//        networked = false;
//
//        diskUndo = true;
//        diskResult = true;
//        traceLevelFile = 3;
//        throttle = 1;
//        cacheType = "SOFT_LRU";
//        cipher = "AES";
//        test();

        diskUndo = false;
        diskResult = false;
        traceLevelFile = 1;
        throttle = 0;
        cacheType = null;
        cipher = null;

//        if (!ci) {
//            traceLevelFile = 0;
//            smallLog = true;
//            networked = true;
//            ssl = true;
//            test();
//
//            big = true;
//            smallLog = false;
//            networked = false;
//            ssl = false;
//            traceLevelFile = 0;
//            test();
//            testAdditional();
//
//            big = false;
//            cipher = "AES";
//            test();
//            cipher = null;
//            test();
//        }

        for (Entry<Class<? extends TestBase>, Boolean> entry : executedTests.entrySet()) {
            if (!entry.getValue()) {
                System.out.println("Warning: test " + entry.getKey().getName() + " was not executed.");
            }
        }
    }

    private void runCoverage() throws SQLException {
        smallLog = big = networked = memory = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        traceTest = stopOnError = false;
        traceLevelFile = throttle = 0;
        cipher = null;

        memory = true;
        test();
        testAdditional();
        testUtils();

        test();
        // testUnit();
    }

    /**
     * Run all tests with the current settings.
     */
    private void test() throws SQLException {
        System.out.println();
        System.out.println("Test " + toString() +
                " (" + Utils.getMemoryUsed() + " KB used)");
        beforeTest();
        try {

            addTest(new TestBatchUpdates());
            addTest(new TestStatement());
            addTest(new TestConnection());
            addTest(new TestTransactionIsolation());
//            addTest(new TestMetaData());//升级db后元数据返回结果改变，暂时忽略

            addTest(new TestCancel());
            addTest(new TestTransaction());

            runAddedTests();



            runAddedTests(1);
        } finally {
            afterTest();
        }
    }

    /**
     * Run additional tests.
     */
    private void testAdditional() {
        if (networked) {
            throw new RuntimeException("testAdditional() is not allowed in networked mode");
        }


        runAddedTests();

        runAddedTests(1);
    }

    /**
     * Run tests for utilities.
     */
    private void testUtils() {
        System.out.println();
        System.out.println("Test utilities (" + Utils.getMemoryUsed() + " KB used)");

        runAddedTests(1);
    }

    private void addTest(TestBase test) {
        if (excludedTests.contains(test.getClass().getName())) {
            return;
        }
        // tests.add(test);
        // run directly for now, because concurrently running tests
        // fails on Raspberry Pi quite often (seems to be a JVM problem)

        // event queue watchdog for tests that get stuck when running in Jenkins
        final java.util.Timer watchdog = new java.util.Timer();
        // 5 minutes
        watchdog.schedule(new TimerTask() {
            @Override
            public void run() {
                ThreadDeadlockDetector.dumpAllThreadsAndLocks("test watchdog timed out");
            }
        }, 5 * 60 * 1000);
        try {
            test.runTest(this);
        } finally {
            watchdog.cancel();
        }
    }

    private void runAddedTests() {
        int threadCount = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
        // threadCount = 2;
        runAddedTests(threadCount);
    }

    private void runAddedTests(int threadCount) {
        Task[] tasks = new Task[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    while (true) {
                        TestBase test;
                        synchronized (tests) {
                            if (tests.isEmpty()) {
                                break;
                            }
                            test = tests.remove(0);
                        }
                        if (!excludedTests.contains(test.getClass().getName())) {
                            test.runTest(TestAll.this);
                        }
                    }
                }
            };
            t.execute();
            tasks[i] = t;
        }
        for (Task t : tasks) {
            t.get();
        }
    }

    private static TestBase createTest(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (TestBase) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception | NoClassDefFoundError e) {
            // ignore
            TestBase.printlnWithTime(0, className + " class not found");
        }
        return new TestBase() {

            @Override
            public void test() throws Exception {
                // ignore
            }

        };
    }

    /**
     * This method is called before a complete set of tests is run. It deletes
     * old database files in the test directory and trace files. It also starts
     * a TCP server if the test uses remote connections.
     */
    public void beforeTest() throws SQLException {
        Driver.load();
        FileUtils.deleteRecursive(TestBase.BASE_TEST_DIR, true);
        DeleteDbFiles.execute(TestBase.BASE_TEST_DIR, null, true);
        FileUtils.deleteRecursive("trace.db", false);
        if (networked) {
            String[] args = ssl ? new String[] { "-ifNotExists", "-tcpSSL" } : new String[] { "-ifNotExists" };
            server = Server.createTcpServer(args);
            try {
                server.start();
            } catch (SQLException e) {
                System.out.println("FAIL: can not start server (may already be running)");
                server = null;
            }
        }
    }

    /**
     * Stop the server if it was started.
     */
    public void afterTest() {
        if (networked && server != null) {
            server.stop();
        }
        FileUtils.deleteRecursive("trace.db", true);
        FileUtils.deleteRecursive(TestBase.BASE_TEST_DIR, true);
    }

    public int getPort() {
        return server == null ? 9192 : server.getPort();
    }

    /**
     * Print system information.
     */
    public static void printSystemInfo() {
        Properties prop = System.getProperties();
        System.out.println("H2 " + Constants.FULL_VERSION +
                " @ " + new java.sql.Timestamp(System.currentTimeMillis()).toString());
        System.out.println("Java " +
                prop.getProperty("java.runtime.version") + ", " +
                prop.getProperty("java.vm.name")+", " +
                prop.getProperty("java.vendor") + ", " +
                prop.getProperty("sun.arch.data.model"));
        System.out.println(
                prop.getProperty("os.name") + ", " +
                        prop.getProperty("os.arch")+", "+
                        prop.getProperty("os.version")+", "+
                        prop.getProperty("sun.os.patch.level")+", "+
                        prop.getProperty("file.separator")+" "+
                        prop.getProperty("path.separator")+" "+
                        StringUtils.javaEncode(prop.getProperty("line.separator")) + " " +
                        prop.getProperty("user.country") + " " +
                        prop.getProperty("user.language") + " " +
                        prop.getProperty("user.timezone") + " " +
                        prop.getProperty("user.variant")+" "+
                        prop.getProperty("file.encoding"));
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        appendIf(buff, lazy, "lazy");
        appendIf(buff, big, "big");
        appendIf(buff, networked, "net");
        appendIf(buff, memory, "memory");
        appendIf(buff, codeCoverage, "codeCoverage");
        appendIf(buff, cipher != null, cipher);
        appendIf(buff, cacheType != null, cacheType);
        appendIf(buff, smallLog, "smallLog");
        appendIf(buff, ssl, "ssl");
        appendIf(buff, diskUndo, "diskUndo");
        appendIf(buff, diskResult, "diskResult");
        appendIf(buff, traceSystemOut, "traceSystemOut");
        appendIf(buff, endless, "endless");
        appendIf(buff, traceLevelFile > 0, "traceLevelFile");
        appendIf(buff, throttle > 0, "throttle:" + throttle);
        appendIf(buff, traceTest, "traceTest");
        appendIf(buff, stopOnError, "stopOnError");
        appendIf(buff, splitFileSystem, "split");
        appendIf(buff, collation != null, collation);
        return buff.toString();
    }

    private static void appendIf(StringBuilder buff, boolean flag, String text) {
        if (flag) {
            buff.append(text);
            buff.append(' ');
        }
    }

}

