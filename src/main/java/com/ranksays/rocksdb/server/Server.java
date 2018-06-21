package com.ranksays.rocksdb.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;

import java.nio.charset.StandardCharsets;


public class Server implements Runnable {

	private final int port;
	private WorkerHandler workerHandler;

	public Server(int port) {
		this.port = port;
	}

	public void run() {
		final EventLoopGroup workerGroup = new NioEventLoopGroup();
		final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		ChannelFuture channelFuture = null;
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
									.addLast(new RequestFilterHandler()) // извлекаем данные из запроса, проверяем
									.addLast(workerHandler); // производим операцию
						}
					})
					.option(ChannelOption.SO_BACKLOG, 500)
					.childOption(ChannelOption.SO_KEEPALIVE, true);

			channelFuture = server.bind("localhost", port).sync();
			channelFuture.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			workerGroup.shutdownGracefully();
			if (channelFuture != null)
				channelFuture.channel().close().awaitUninterruptibly();
		}
	}
	public static void sendError(ChannelHandlerContext ctx, String errorMessage, HttpResponseStatus status) {
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