package com.ranksays.rocksdb.server;

import com.ranksays.rocksdb.server.handlers.GetHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;


public class Server implements Runnable {

	private static final Logger logger = LogManager.getLogger(Server.class);

	private final int port;
	private WorkerHandler workerHandler;
	private ChannelFuture sync;

	public Server(int port) {
		this.port = port;
	}

	public void run() {
		final EventLoopGroup workerGroup = new NioEventLoopGroup();
		final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		try {
			final ServerBootstrap server = new ServerBootstrap()
					.group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class)
					.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel ch) {

							ch.pipeline()
									.addLast(new HttpResponseEncoder())
									.addLast(new HttpRequestDecoder())
									.addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
									.addLast(new RequestFilterHandler())
									.addLast(workerHandler);
						}
					})
					.option(ChannelOption.SO_BACKLOG, 500)
//					.option(ChannelOption.SO_TIMEOUT, 2_000)
					.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2_000)
					.childOption(ChannelOption.SO_KEEPALIVE, true);

			sync = server.bind("localhost", port).sync();
			sync.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			logger.info(e.getMessage(), e);
		} finally {
			logger.info("shutdownGracefully");
			if (sync != null)
				sync.channel().close().awaitUninterruptibly();
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	static void sendError(ChannelHandlerContext ctx, String errorMessage, HttpResponseStatus status) {
		final ByteBuf content = Unpooled.copiedBuffer(errorMessage, StandardCharsets.UTF_8);
		final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);

		response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
		response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
		response.headers().set(HttpHeaderNames.ACCEPT_CHARSET, StandardCharsets.UTF_8.name());

		final ChannelFuture channelFuture = ctx.writeAndFlush(response);
		channelFuture.addListener(ChannelFutureListener.CLOSE);
	}

	public void setHandler(WorkerHandler workerHandler) {
		this.workerHandler = workerHandler;
	}
}