package com.ranksays.rocksdb.server;

import com.ranksays.rocksdb.server.handlers.GetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.ranksays.rocksdb.server.Utils.deleteFile;

public class RocksDbWrapper {
	private static final Logger logger = LogManager.getLogger(RocksDbWrapper.class);
	private final Map<String, RocksDB> openDBs;

	public RocksDbWrapper(Map<String, RocksDB> openDBs) {

		this.openDBs = openDBs;
	}

	/**
	 * Open a specified database (create if missing).
	 *
	 * @param db   the DB name
	 * @param resp response container
	 * @return the RocksDB reference or null if failed.
	 */
	public RocksDB openDatabase(String db, Response resp) {
		synchronized (openDBs) {
			if (openDBs.containsKey(db)) {
				return openDBs.get(db);
			} else {
				Options opts = new Options();
				opts.setCreateIfMissing(true);

				RocksDB rdb = null;
				try {
					rdb = RocksDB.open(opts, Configs.dataDir + "/" + db);
					openDBs.put(db, rdb);
					return rdb;
				} catch (RocksDBException e) {
					String msg = "Failed to open database: " + e.getMessage();
					logger.error(msg);

					resp.setCode(Response.CODE_FAILED_TO_OPEN_DB);
					resp.setMessage(msg);
				}
				// If user doesn't call options dispose explicitly, then
				// this options instance will be GC'd automatically.
				// opts.close();
			}
		}

		return null;
	}

	public void dropIfExist(String db) throws IOException {
		synchronized (openDBs) {
			if (openDBs.containsKey(db)) {
				openDBs.get(db).close();
				openDBs.remove(db);
			}

			File f = new File(Configs.dataDir, db);
			deleteFile(f);
		}
	}
}
