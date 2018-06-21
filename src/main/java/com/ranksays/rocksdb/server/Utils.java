package com.ranksays.rocksdb.server;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Utils {

	/**
	 * Basic authorization
	 *
	 * @param req
	 *            HTTP request header
	 * @return true if authorized otherwise false.
	 */
	public static boolean authorize(JSONObject req, Response resp) {
		if (Configs.authEnabled) {
			if (!req.isNull("auth")) {
				try {
					byte[] decoded = Base64.getDecoder().decode(req.getString("auth"));
					String[] tokens = new String(decoded, StandardCharsets.UTF_8).split(":");

					if (Configs.username.equals(tokens[0]) && Configs.password.equals(tokens[1])) {
						return true;
					}
				} catch (Exception e) {
					// do nothing
				}
			}
		} else {
			return true;
		}

		resp.setCode(Response.CODE_UNAUTHORIZED);
		resp.setMessage("Not authorized");
		return false;
	}

	/**
	 * Parse database name from the request.
	 *
	 * @param req
	 *            request body
	 * @param resp
	 *            response container
	 * @return a valid DB name; or null if the 'db' key does not exist or the
	 *         corresponding value is JSONObject.NULL or does not match the
	 *         specifications.
	 */
	public   static String parseDB(JSONObject req, Response resp) {

		if (!req.isNull("db")) {
			String db = req.getString("db");

			if (db.matches("[a-zA-Z0-9_]+")) {
				return db;
			}
		}

		resp.setCode(Response.CODE_INVALID_DB_NAME);
		resp.setMessage("Invalid database name");
		return null;
	}

	/**
	 * parse key(s) from the request.
	 *
	 * @param req
	 *            request body
	 * @param resp
	 *            response container
	 * @return a byte array; or null if the 'keys' key does not exist or the
	 *         corresponding value is JSONObject.NULL or is not a JSONArray of
	 *         Base64-encoded strings.
	 */
	public static byte[][] parseKeys(JSONObject req, Response resp) {

		if (!req.isNull("keys")) {
			try {
				JSONArray arr = req.getJSONArray("keys");

				byte[][] result = new byte[arr.length()][];
				for (int i = 0; i < arr.length(); i++) {
					result[i] = Base64.getDecoder().decode(arr.getString(i));
				}
				return result;
			} catch (Exception e) {
				// do nothing
			}
		}

		resp.setCode(Response.CODE_INVALID_KEY);
		resp.setMessage("Invalid key(s)");
		return null;
	}

	/**
	 * parse value(s) from the request.
	 *
	 * @param req
	 *            request body
	 * @param resp
	 *            response container
	 * @return a byte array; or null if the 'values' key does not exist or the
	 *         corresponding value is JSONObject.NULL or is not a JSONArray of
	 *         Base64-encoded strings.
	 */
	public static byte[][] parseValues(JSONObject req, Response resp) {

		if (!req.isNull("values")) {
			try {
				JSONArray arr = req.getJSONArray("values");

				byte[][] result = new byte[arr.length()][];
				for (int i = 0; i < arr.length(); i++) {
					result[i] = Base64.getDecoder().decode(arr.getString(i));
				}
				return result;
			} catch (Exception e) {
				// do nothing
			}
		}

		resp.setCode(Response.CODE_INVALID_VALUE);
		resp.setMessage("Invalid value(s)");
		return null;
	}

	/**
	 * Delete file or directory recursively.
	 *
	 * @param f
	 *            file or directory to delete
	 * @throws IOException
	 *             failed to delete a file
	 */
	static void deleteFile(File f) throws IOException {
		if (f.exists()) {
			if (f.isDirectory()) {
				for (File c : f.listFiles()) {
					deleteFile(c);
				}
			}
			if (!f.delete()) {
				throw new IOException("Failed to delete file: " + f);
			}
		}
	}

}
