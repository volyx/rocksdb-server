package com.ranksays.rocksdb.server;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.HeadersUtils;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.router.Router;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;

import static com.ranksays.rocksdb.server.Utils.*;

@ChannelHandler.Sharable
public class WorkerHandler extends ChannelInboundHandlerAdapter {

	private static final Logger logger = LogManager.getLogger(WorkerHandler.class);

	private final Router<Function<JSONObject, Response>> router;

	public WorkerHandler(Router<Function<JSONObject, Response>> router) {
		super();
		this.router = router;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object obj) {
		final FullHttpRequest request = (FullHttpRequest) obj;
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);

		String target = request.uri();
		Response resp = null;


		try {
			// parse request
			ByteArrayOutputStream buf = new ByteArrayOutputStream();
			ByteArrayInputStream in = new ByteArrayInputStream(request.content().array());
			for (int c; (c = in.read()) != -1;) {
				buf.write(c);
			}
			JSONObject req = new JSONObject(buf.size() > 0 ? buf.toString(StandardCharsets.UTF_8.name()) : "{}");
			;
			// authorization (Basic Auth prioritizes)
			String basicAuth = request.headers().get("Authorization");
			if (basicAuth != null && basicAuth.startsWith("Basic ")) {
				req.put("auth", basicAuth.substring(6));
			}

			resp = router.route(request.method(), request.uri()).target().apply(req);

		} catch (Exception e) {
			final ByteBuf INTERNAL_ERROR = PooledByteBufAllocator.DEFAULT.buffer().writeBytes("Internal server error".getBytes());


			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, INTERNAL_ERROR);
			logger.error("Internal error: " + e.getMessage());
		}

		response.headers().set("ContentType", "application/json; charset=utf-8");
		response.setStatus(HttpResponseStatus.OK);
		response.content().writeBytes(resp.toJSON().toString().getBytes());

//		baseRequest.setHandled(true);

		final ChannelFuture channelFuture = ctx.writeAndFlush(response);
		channelFuture.addListener(ChannelFutureListener.CLOSE);

		request.release();
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//		if (cause instanceof NoAccountRegisteredException) {
//			// Если не нашли счет с таким id
//			final String accountId = cause.getMessage();
//			Server.sendError(ctx, "account with id " + accountId + " not found", HttpResponseStatus.NOT_FOUND);
//		}
//		if (cause instanceof InsufficientFundsException) {
//			// Если не хватило денег
//			final String accountId = cause.getMessage();
//			Server.sendError(ctx, "on account with id " + accountId + " not enough funds",
//					HttpResponseStatus.FORBIDDEN);
//		} else {
			Server.sendError(ctx, "internal server error: " + cause.getMessage(),
					HttpResponseStatus.INTERNAL_SERVER_ERROR);
//		}
	}

}