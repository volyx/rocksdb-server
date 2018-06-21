package com.ranksays.rocksdb.server;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.ranksays.rocksdb.server.handlers.DropDatabaseHandler;
import com.ranksays.rocksdb.server.handlers.GetHandler;
import com.ranksays.rocksdb.server.handlers.PutHandler;
import com.ranksays.rocksdb.server.handlers.RemoveHandler;
import io.netty.handler.codec.http.router.Router;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.rocksdb.RocksDB;

/**
 * Launcher for RocksDB Server
 *
 */
public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    private static Map<String, RocksDB> databases = new HashMap<>();

    public static void main(String[] args) throws Exception {

        if (Configs.load()) {
            // Load RocksDB static libarary
            RocksDB.loadLibrary();

            // Create data folder if missing
            new File(Configs.dataDir).mkdirs();

            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    synchronized (databases) {
                        for (RocksDB db : databases.values()) {
                            if (db != null) {
                                db.close();
                            }
                        }
                        logger.info("Server shut down.");
                    }
                }
            });

            RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(databases);

            Router<Function<JSONObject, Response>> router = new Router<Function<JSONObject, Response>>()
                    .GET("/get", new GetHandler(rocksDbWrapper))
                    .GET("/put", new PutHandler(rocksDbWrapper))
                    .GET("/remove", new RemoveHandler(rocksDbWrapper))
                    .GET("/drop_database", new DropDatabaseHandler(rocksDbWrapper))
					.ANY("/", jsonObject -> new Response("This rocksdb-server is working"))

					;

//			if ("/get".equals(target)) {
//				resp = doGet(req);
//			} else if ("/put".equals(target)) {
//				resp = doPut(req);
//			} else if ("/remove".equals(target)) {
//				resp = doRemove(req);
//			} else if ("/drop_database".equals(target)) {
//				resp = doDropDatabase(req);
//			} else if ("/".equals(target)) {
//				resp = new Response("This rocksdb-server is working");
//			} else {
//				resp = new Response(404, "Sorry, that page does not exist");
//			}

            System.out.println(router);

            // Create Server
//            InetSocketAddress addr = new InetSocketAddress(Configs.listen, Configs.port);
            Server server = new Server(Configs.port);
            server.setHandler(new WorkerHandler(router));

//            try {
                server.run();
                logger.info("Server started.");
                Thread.currentThread().join();
//            } catch (BindException e) {
//                logger.error("Failed to bind at " + addr + ": " + e.getMessage());
//                System.exit(-1);
//            }
        }
    }
}
