package com.ranksays.rocksdb.server.handlers;

import com.ranksays.rocksdb.server.Response;
import com.ranksays.rocksdb.server.RocksDbWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Base64;
import java.util.function.Function;

import static com.ranksays.rocksdb.server.Utils.*;

public class GetHandler implements Function<JSONObject, Response> {

	private static final Logger logger = LogManager.getLogger(GetHandler.class);
	private RocksDbWrapper rocksDbWrapper;

	public GetHandler(RocksDbWrapper rocksDbWrapper) {
		this.rocksDbWrapper = rocksDbWrapper;
	}

	@Override
	public Response apply(JSONObject req) {
		Response resp = new Response();

		// parse parameters & open database
		String db = null;
		byte[][] keys = null;
		RocksDB rdb = null;
		if (!authorize(req, resp) || (db = parseDB(req, resp)) == null || (keys = parseKeys(req, resp)) == null
				|| (rdb = rocksDbWrapper.openDatabase(db, resp)) == null) {
			return resp;
		}

		try {
			byte[][] values = new byte[keys.length][];
			for (int i = 0; i < keys.length; i++) {
				values[i] = rdb.get(keys[i]);
			}

			JSONArray arr = new JSONArray();
			for (int i = 0; i < keys.length; i++) {
				if (values[i] == null) {
					arr.put(JSONObject.NULL);
				} else {
					arr.put(Base64.getEncoder().encodeToString(values[i]));
				}
			}
			resp.setResults(arr);
		} catch (RocksDBException e) {
			resp.setCode(Response.CODE_INTERNAL_ERROR);
			resp.setMessage("Internal server error");

			logger.error("get operation failed: " + e.getMessage());
		}

		return resp;
	}
}
