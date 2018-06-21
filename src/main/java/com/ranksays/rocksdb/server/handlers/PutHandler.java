package com.ranksays.rocksdb.server.handlers;

import com.ranksays.rocksdb.server.Response;
import com.ranksays.rocksdb.server.RocksDbWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.function.Function;

import static com.ranksays.rocksdb.server.Utils.*;

public class PutHandler implements Function<JSONObject, Response> {
	private static final Logger logger = LogManager.getLogger(GetHandler.class);
	private RocksDbWrapper rocksDbWrapper;

	public PutHandler(RocksDbWrapper rocksDbWrapper) {

		this.rocksDbWrapper = rocksDbWrapper;
	}

	@Override
	public Response apply(JSONObject req) {

		Response resp = new Response();

		// parse parameters & open database
		String db = null;
		byte[][] keys = null, values = null;
		RocksDB rdb = null;
		if (!authorize(req, resp) || (db = parseDB(req, resp)) == null || (keys = parseKeys(req, resp)) == null
				|| (values = parseValues(req, resp)) == null || (rdb = rocksDbWrapper.openDatabase(db, resp)) == null) {
			return resp;
		}

		if (keys.length != values.length) {
			resp.setCode(Response.CODE_KEY_VALUE_MISMATCH);
			resp.setMessage("Number of key and value does not match");
			return resp;
		}

		try {
			for (int i = 0; i < keys.length; i++) {
				rdb.put(keys[i], values[i]);
			}
		} catch (RocksDBException e) {
			resp.setCode(Response.CODE_INTERNAL_ERROR);
			resp.setMessage("Internal server error");

			logger.error("put operation failed: " + e.getMessage());
		}

		return resp;

	}
}
