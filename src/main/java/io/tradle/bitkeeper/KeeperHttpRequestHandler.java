package io.tradle.bitkeeper;

import static org.jboss.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public class KeeperHttpRequestHandler extends SimpleChannelUpstreamHandler {

	private HttpRequest request;

	private boolean readingChunks;

	private Peer peer;

	private ExecutorService threadPool;

	private Gson gson = new GsonBuilder().setPrettyPrinting()
			.disableHtmlEscaping().create();

	/** Buffer that stores the response content */
	private final StringBuilder buf = new StringBuilder();

	private static final JsonPrimitive JTRUE = new JsonPrimitive(true);
	private static final JsonPrimitive JFALSE = new JsonPrimitive(false);
	
	public KeeperHttpRequestHandler(Peer peer, ExecutorService threadPool) {
		this.peer = peer;
		this.threadPool = threadPool;
	}

	/**
	 * Handles two types of HTTP requests. 1. For request with a single
	 * parameter 'key' returns associated data found in DHT 2. For request with
	 * two parameters 'key' and 'val' places the key value pair into DHT
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e)throws Exception {
		HttpRequest request = this.request = (HttpRequest) e.getMessage();

		if (is100ContinueExpected(request)) {
			send100Continue(e);
		}

		buf.setLength(0);

		QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
		Map<String, List<String>> params = queryStringDecoder.getParameters();
		if (params.isEmpty()) {
			writeResponse(e, false);
			return;
		}

		List<String> keyL = params.get("key");
		List<String> valL = params.get("val");
		List<String> mKeys = params.get("keys");
		final String k = keyL == null ? null : keyL.get(0);
		final String v = valL == null ? null : valL.get(0);
		final String[] keys = mKeys == null ? null : mKeys.get(0).split(",");

		if (k == null && keys == null) {
			writeResponse(e, false);
			return;
		}

		if (keys != null) {
			multiget(e, keys);
			return;
		}

		DHTQuery query = new DHTQuery(k, v);
		query.addListener(new BaseFutureAdapter<FutureDHT>() {
			public void operationComplete(FutureDHT futureDHT) throws Exception {
				boolean ok = futureDHT.isSuccess();
				if (!ok) {
					System.err.println("Value not found for key " + k + ": "
							+ futureDHT.getFailedReason());
					// TODO set return status 404
				} else {
					if (v == null) {
						Map<Number160, Data> map = futureDHT.getDataMap();

						for (Data data : map.values()) {
							String value = (String) data.getObject();
							buf.append(value);
						}
					} else {
						System.err.println("Value stored for key " + k);
						buf.append("OK");
					}
				}

				writeResponse(e, ok);
			}
		});

		threadPool.execute(query);
	}

	private void multiget(MessageEvent e, final String[] keys) {
		final List<String> results = new ArrayList<String>();
		DHTQuery[] queries = new DHTQuery[keys.length];
		final CountDownLatch latch = new CountDownLatch(keys.length);
		for (int i = 0; i < keys.length; i++) {
			results.add(null);
			queries[i] = new DHTQuery(keys[i], null);
			final int idx = i;
			queries[i].addListener(new BaseFutureAdapter<FutureDHT>() {
				public void operationComplete(FutureDHT futureDHT) throws Exception {					
					if (!futureDHT.isSuccess()) {
						System.err.println("Value not found for key "
								+ keys[idx] + ": "
								+ futureDHT.getFailedReason());
					} else {
						Map<Number160, Data> map = futureDHT.getDataMap();
						for (Number160 n : map.keySet()) {
							results.set(idx, (String) map.get(n).getObject());
							break;
						}

					}
					
					latch.countDown();
				}
			});

			threadPool.execute(queries[i]);
		}
		
		try {
			latch.await();
		} catch (InterruptedException e1) {
			writeResponse(e, false);
			return;
		}
		
		gson.toJson(results, buf);
		writeResponse(e, true);
	}

	private void multiput(MessageEvent e, Map<String, String> keyValMap) {
		int size = keyValMap.size();
		int i = 0;
		final JsonArray results = new JsonArray();
		DHTQuery[] queries = new DHTQuery[size];
		final CountDownLatch latch = new CountDownLatch(size);
		for (String key: keyValMap.keySet()) {
			final int idx = i;
			results.add(JFALSE);
			queries[i] = new DHTQuery(key, keyValMap.get(key));			
			queries[i].addListener(new BaseFutureAdapter<FutureDHT>() {
				public void operationComplete(FutureDHT futureDHT) throws Exception {					
					if (futureDHT.isSuccess())
						results.set(idx, JTRUE);
					else
						System.err.println("Put failed: " + futureDHT.getFailedReason());
					
					latch.countDown();
				}
			});

			threadPool.execute(queries[i++]);
		}
		
		try {
			latch.await();
		} catch (InterruptedException e1) {
			writeResponse(e, false);
			return;
		}
		
		buf.append(gson.toJson(results));
		writeResponse(e, true);
	}
	private void writeResponse(MessageEvent e, boolean ok) {
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, ok ? OK
				: NOT_FOUND);
		response.setContent(ChannelBuffers.copiedBuffer(buf.toString(),
				CharsetUtil.UTF_8));
		response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

		if (keepAlive) {
			// Add 'Content-Length' header only for a keep-alive connection.
			response.setHeader(CONTENT_LENGTH, response.getContent()
					.readableBytes());
		}

		// Write the response.
		ChannelFuture future = e.getChannel().write(response);

		// Close the non-keep-alive connection after the write operation is
		// done.
		if (!keepAlive) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void send100Continue(MessageEvent e) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
		e.getChannel().write(response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		// e.getCause().printStackTrace();
		e.getChannel().close();
	}

	class DHTQuery implements Runnable {
		private String key;

		private String val;

		private FutureDHT dht;

		private List<BaseFutureAdapter<FutureDHT>> listeners;

		DHTQuery(String key, String val) {
			this.key = key;
			this.val = val;
		}

		void addListener(BaseFutureAdapter<FutureDHT> listener) {
			if (this.listeners == null)
				this.listeners = new ArrayList<BaseFutureAdapter<FutureDHT>>();

			this.listeners.add(listener);
		}

		FutureDHT getFuture() {
			return dht;
		}

		public void run() {
			if (val != null) {
				try {
					dht = put(key, val);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				dht = get(key);
			}

			if (listeners != null) {
				for (BaseFutureAdapter<FutureDHT> listener : listeners) {
					dht.addListener(listener);
				}
			}
		}

		private FutureDHT put(String key, String data) throws IOException {
			Number160 locationKey = Number160.createHash(key);

			// TODO: upgrade TomP2P, use putIfAbsent().putConfirm() and issue
			// 409 if key was already there
			FutureDHT dht = peer.put(locationKey).setData(new Data(data)).start();
			return dht;
		}

		private FutureDHT get(String key) {
			Number160 locationKey = Number160.createHash(key);
			FutureDHT dht = peer.get(locationKey).start();
			return dht;
		}
	}
}
