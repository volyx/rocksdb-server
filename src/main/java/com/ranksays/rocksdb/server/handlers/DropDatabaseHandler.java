package com.ranksays.rocksdb.server.handlers;

import com.ranksays.rocksdb.server.Response;
import com.ranksays.rocksdb.server.RocksDbWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.util.function.Function;

import static com.ranksays.rocksdb.server.Utils.authorize;
import static com.ranksays.rocksdb.server.Utils.parseDB;

public class DropDatabaseHandler implements Function<JSONObject, Response> {
	private static final Logger logger = LogManager.getLogger(GetHandler.class);
	private RocksDbWrapper rocksDbWrapper;

	public DropDatabaseHandler(RocksDbWrapper rocksDbWrapper) {

		this.rocksDbWrapper = rocksDbWrapper;
	}

	@Override
	public Response apply(JSONObject req) {
		Response resp = new Response();

		// parse parameters
		String db = null;
		if (!authorize(req, resp) || (db = parseDB(req, resp)) == null) {
			return resp;
		}

		try {
			rocksDbWrapper.dropIfExist(db);
		} catch (IOException e) {
			resp.setCode(Response.CODE_INTERNAL_ERROR);
			resp.setMessage("Internal server error");
			logger.error("dropping database failed: " + e.getMessage());
		}

		return resp;
	}
}
