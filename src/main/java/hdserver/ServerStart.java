package hdserver;

import org.h2.tools.Server;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class ServerStart implements SignalHandler {

    private Server server = null;


    @Override
    public void handle(Signal signal) {
        System.out.println("handle signal:" + signal.getName());
        if (server != null) {
            System.out.println("server stop...");
            server.stop();
            System.out.println("server stop finish");
        }
    }


    public void run(String cfgPath) {
        try {
            DBConfig.load(cfgPath);
            Log.init(DBConfig.getInstance().logDir + "/db.log",
                DBConfig.getInstance().logFileRowLimit,
                DBConfig.getInstance().logFileCount);

            TcpServer tcpServer = new TcpServer();
            int port = DBConfig.getInstance().port;
            server = new Server(tcpServer, new String[] {"-tcpPort","" + port,"-ifNotExists", "-tcpAllowOthers"});
            tcpServer.setShutdownHandler(server);
            server.start();
            Log.info("start server finish:");
        } catch(Exception e) {
            System.out.println("ServerStart Error:" + e.getMessage());
        }
    }


}
