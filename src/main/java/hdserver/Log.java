package hdserver;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

public class Log {
    private static Logger log = Logger.getGlobal();


    public static void init(String path, int limit, int count) {
        log.setUseParentHandlers(false);
        Formatter formatter = new Formatter() {
            @Override
            public String format(LogRecord record) {
                Date d = new Date(record.getMillis());
                SimpleDateFormat dm= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                return String.format("[%s][tid=%d][%s]:%s\r\n",
                        dm.format(d),
                        record.getThreadID(),
                        record.getLevel().toString(),
                        record.getMessage());
            }
        };
        try {
            FileHandler fileHandler = new FileHandler(path, limit, count);
            ConsoleHandler consoleHandler = new ConsoleHandler();
            fileHandler.setFormatter(formatter);
            consoleHandler.setFormatter(formatter);
            log.addHandler(fileHandler);
            log.addHandler(consoleHandler);
        } catch (Exception e) {
            System.out.println("log init error:" + path);
        }
    }

    public static void info(String msg) {
        log.info(msg);
    }

    public static void warning(String msg) {
        log.warning(msg);
    }

    public static void error(String msg) {
        log.log(Level.SEVERE, msg);
    }

    public static void detail(String msg) {
        log.log(Level.FINE, msg);
    }

    public static void config(String msg) {
        log.log(Level.CONFIG, msg);
    }

}
