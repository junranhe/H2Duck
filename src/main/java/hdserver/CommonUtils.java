package hdserver;

import org.h2.store.fs.FileUtils;
import org.h2.tools.DeleteDbFiles;

import java.io.File;

public class CommonUtils {
    public static void deleteDb(String dir, String name) {
        DeleteDbFiles.execute(dir, name, true);

        String filename = FileUtils.toRealPath(dir + "/" + name);
        FileUtils.tryDelete(filename);

        deleteFile(new File(filename+".tmp"));
        FileUtils.tryDelete(filename+".wal");

    }

    public static void createDir(String path) {
         FileUtils.createDirectory(path);
    }

    public static boolean existsFileOrDir(String path) {
        return FileUtils.exists(path);
    }

    public static boolean isEmptyDir(String path) {
        File f = new File(path);
        if (!f.exists())
            return false;
        if (!f.isDirectory())
            return false;
        File[] files = f.listFiles();
        return files.length == 0;
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
}
