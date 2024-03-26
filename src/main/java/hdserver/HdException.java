package hdserver;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

public class HdException {
    public static void check(boolean condition, String msgFormat, Object ...args) {
        if (!condition) {
            String msg = String.format(msgFormat, args);
            Log.info(msg);
            throw DbException.get(ErrorCode.GENERAL_ERROR_1, msg);
        }
    }

    public static DbException get(String msgFormat, Object ...args) {
        String msg = String.format(msgFormat, args);
        Log.info(msg);
        return DbException.get(ErrorCode.GENERAL_ERROR_1, msg);
    }

    public static DbException getUnsupportedException(String msgFormat, Object ...args) {
        String msg = String.format(msgFormat, args);
        Log.info(msg);
        return DbException.getUnsupportedException( msg);
    }

    public static DbException convert(Throwable e, String msgFormat, Object ...args) {
        String msg = String.format(msgFormat, args);
        Log.warning(msg);
        e.printStackTrace();
        return DbException.convert(e);
    }
}
