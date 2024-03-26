import hdserver.ServerStart;
import sun.misc.Signal;

import java.util.ArrayList;
import java.util.Arrays;

public class Launcher {



    public static void main(String[] args) {
        if(args.length < 2 ||
                args[0] == null ||
                args[1] == null) {
            System.out.println("args error:" + Arrays.toString(args));
            return;
        }

        String cmd = args[0];
        String cfgPath = args[1];
        ArrayList<String> cmdArgs = new ArrayList<String>();
        for (int i = 1; i < args.length; i++)
            cmdArgs.add(args[i]);

        if (cmd.equals("startserver")) {
            ServerStart serverStart = new ServerStart();
            serverStart.run(cfgPath);
            Signal.handle(new Signal("TERM"), serverStart);
            Signal.handle(new Signal("INT"), serverStart);
        } else {
            System.out.println("Unknow command:" + Arrays.toString(args));
        }
    }
}
