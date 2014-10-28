package io.tradle.bitkeeper;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import net.tomp2p.futures.*;
import net.tomp2p.p2p.*;
import net.tomp2p.peers.*;
import net.tomp2p.storage.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.*;

public class KeeperHttpRequestHandler extends SimpleChannelUpstreamHandler {

  private HttpRequest         request;

  private boolean             readingChunks;

  private Peer                peer;

  private ExecutorService     threadPool;

  /** Buffer that stores the response content */
  private final StringBuilder buf = new StringBuilder();

  public KeeperHttpRequestHandler(Peer peer, ExecutorService threadPool) {
    this.peer = peer;
    this.threadPool = threadPool;
  }
  
  
  /*
   * Handles two types of HTTP requests. 
   * 1. For request with a single parameter 'key' returns associated data found in DHT
   * 2. For request with two parameters 'key' and 'value' places the key value pair into DHT
   */
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    HttpRequest request = this.request = (HttpRequest) e.getMessage();

    if (is100ContinueExpected(request)) {
      send100Continue(e);
    }

    buf.setLength(0);

    boolean ok = false;
    QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
    Map<String, List<String>> params = queryStringDecoder.getParameters();
    if (!params.isEmpty()) {
      String k = null;
      String v = null;
      for (Entry<String, List<String>> p : params.entrySet()) {
        String key = p.getKey();
        List<String> vals = p.getValue();
        for (String val : vals) {
          if (key.equals("key"))
            k = val;
          else if (key.equals("val"))
            v = val;
        }
      }
      
      if (k != null) {
        DHTQuery query = new DHTQuery(k, v);
        threadPool.execute(query);
        synchronized (this) {
          wait();
        }
        FutureDHT futureDHT = query.getFuture();
        if (futureDHT.isFailed()) {
          System.err.println("Value not found for key " + k + ": " + futureDHT.getFailedReason());
          //TODO set return status 404
        }  
        else {
          ok = true;
          if (v == null) {
            Map<Number160, Data> map = futureDHT.getDataMap();
            for (Data data : map.values()) {
              String value = (String) data.getObject();
              buf.append(value);
            }
          }
          else {
            System.err.println("Value stored for key " + k);
            buf.append("OK");
          }
        }
      }
    }
    
    writeResponse(e, ok);
  }

  private void writeResponse(MessageEvent e, boolean ok) {
    // Decide whether to close the connection or not.
    boolean keepAlive = isKeepAlive(request);

    // Build the response object.
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, ok ? OK : NOT_FOUND);
    response.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

    if (keepAlive) {
      // Add 'Content-Length' header only for a keep-alive connection.
      response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
    }

    // Write the response.
    ChannelFuture future = e.getChannel().write(response);

    // Close the non-keep-alive connection after the write operation is done.
    if (!keepAlive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  private void send100Continue(MessageEvent e) {
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
    e.getChannel().write(response);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    // e.getCause().printStackTrace();
    e.getChannel().close();
  }

  class DHTQuery implements Runnable {
    private String    key;

    private String    data;

    private FutureDHT dht;

    DHTQuery(String key, String data) {
      this.key = key;
      this.data = data;
    }

    FutureDHT getFuture() {
      return dht;
    }

    @Override
    public void run() {
      if (data != null) {
        try {
          dht = put(key, data);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        dht = get(key);
      }
      synchronized (KeeperHttpRequestHandler.this) {
        KeeperHttpRequestHandler.this.notify();
      }
    }

    private FutureDHT put(String key, String data) throws IOException {
      Number160 locationKey = Number160.createHash(key);
      FutureDHT dht = peer.put(locationKey).setData(new Data(data)).start();
      dht.awaitUninterruptibly();
      return dht;
    }

    private FutureDHT get(String key) {
      Number160 locationKey = Number160.createHash(key);
      FutureDHT dht = peer.get(locationKey).start();
      dht.awaitUninterruptibly();
      return dht;
    }
  }
}
