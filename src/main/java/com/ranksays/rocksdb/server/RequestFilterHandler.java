package com.ranksays.rocksdb.server;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RequestFilterHandler extends MessageToMessageDecoder<FullHttpRequest> {
	private static final String ACCOUNTS_URL = "accounts";

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		Server.sendError(ctx, "internal server error: " + cause.getMessage(),
				HttpResponseStatus.INTERNAL_SERVER_ERROR);
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, FullHttpRequest request, List<Object> out) {
		final HttpMethod method = request.method();

		out.add(request);
		request.retain();

		// Поддерживается только GET для запроса баланса и POST для всего остального
//		if (method == HttpMethod.GET) handleGet(ctx, request, out);
//		else if (method == HttpMethod.POST) handlePost(ctx, request, out);
//		else Server.sendError(ctx, "method is not acceptable", HttpResponseStatus.NOT_ACCEPTABLE);
	}

//	/**
//	 * Обрабатывает запросы типа 'GET /accounts/123', где 123 - id счета. Складывает в контекст только
//	 * код операции (balance) и id счета.
//	 */
//	private void handleGet(ChannelHandlerContext ctx, FullHttpRequest request, List<Object> out) {
//		final List<String> urlParts = getUrlParts(request);
//		if (urlParts != null && urlParts.size() == 2 && ACCOUNTS_URL.equals(urlParts.get(0))) {
//			final Long accountId = parseAndCheck(urlParts.get(1));
//			if (accountId != null) {
//				ctx.channel().attr(KEY_ACCOUNT_ID).set(accountId);
//				ctx.channel().attr(KEY_OPERATION).set(OPER_BALANCE);
//				out.add(request);
//				request.retain();
//			} else
//				Server.sendError(ctx, "invalid account id", HttpResponseStatus.BAD_REQUEST);
//		} else
//			send404(ctx);
//	}
//
//	/**
//	 * Обрабатывает запросы типа 'POST /accounts/123/withdraw', где 123 - id счета, а withdraw - операция.
//	 * Складывает в контекст код операции, сумму и счет назначения (если это перевод).
//	 */
//	private void handlePost(ChannelHandlerContext ctx, FullHttpRequest request, List<Object> out) {
//		// Достаем сумму операции из тела запроса
//		final ByteBuf content = request.content();
//		if (content.isReadable()) {
//			final String contentString = content.readCharSequence(content.readableBytes(), CharsetUtil.UTF_8).toString();
//			final Long amount = parseAndCheck(contentString);
//			if (amount != null)
//				ctx.channel().attr(KEY_AMOUNT).set(amount);
//			else {
//				Server.sendError(ctx, "invalid amount", HttpResponseStatus.BAD_REQUEST);
//				return;
//			}
//		} else {
//			Server.sendError(ctx, "invalid amount", HttpResponseStatus.BAD_REQUEST);
//			return;
//		}
//
//		final List<String> urlParts = getUrlParts(request);
//		if (urlParts == null || urlParts.size() < 3 || !ACCOUNTS_URL.equals(urlParts.get(0))) {
//			send404(ctx);
//			return;
//		}
//
//		final String operation = urlParts.get(2);
//		switch (operation) {
//			case OPER_TRANSFER:
//				// Для перевода требуется id счета назначения
//				if (urlParts.size() > 3) {
//					final Long destinationId = parseAndCheck(urlParts.get(3));
//					if (destinationId != null)
//						ctx.channel().attr(KEY_DESTINATION_ID).set(destinationId);
//					else {
//						Server.sendError(ctx, "invalid destination id", HttpResponseStatus.BAD_REQUEST);
//						break;
//					}
//				}
//				else {
//					Server.sendError(ctx, "no destination for transfer", HttpResponseStatus.BAD_REQUEST);
//					break;
//				}
//			case OPER_DEPOSIT:
//			case OPER_WITHDRAW:
//				final Long accountId = parseAndCheck(urlParts.get(1));
//				if (accountId != null) {
//					ctx.channel().attr(KEY_ACCOUNT_ID).set(accountId);
//					ctx.channel().attr(KEY_OPERATION).set(operation);
//					out.add(request);
//					request.retain();
//				} else
//					Server.sendError(ctx, "invalid account id", HttpResponseStatus.BAD_REQUEST);
//				break;
//			default:
//				// Если такой операции нет для POST - 404
//				send404(ctx);
//		}
//	}

	/**
	 * Отправляет код 404
	 */
	private void send404(ChannelHandlerContext ctx) {
		Server.sendError(ctx, "resource is not found", HttpResponseStatus.NOT_FOUND);
	}

	/**
	 * Добывает из запроса путь к ресурсу в виде списка
	 */
	private List<String> getUrlParts(FullHttpRequest request) {
		final String url = request.uri();
		return url == null ? null :
				Arrays.stream(url.toLowerCase().split("/"))
						.filter(part -> part != null && !part.isEmpty())
						.collect(Collectors.toList());
	}

	/**
	 * Парсит строку в UnsignedLong, получившееся число должно быть больше нуля
	 * @return число больше нуля, null - в противном случае или при ошибке
	 */
	private static Long parseAndCheck(String s) {
		try {
			final Long value = Long.parseUnsignedLong(s);
			return value > 0 ? value : null;
		} catch (NumberFormatException e) {
			return null;
		}
	}
}