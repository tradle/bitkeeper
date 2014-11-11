package io.tradle.bitkeeper;

import static org.jboss.netty.channel.Channels.*;

import java.util.concurrent.*;

import net.tomp2p.p2p.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

 
public class ServerPipelineFactory implements ChannelPipelineFactory {
     private Peer peer;
     private ExecutorService threadPool;
     
     ServerPipelineFactory(Peer peer, ExecutorService threadPool) {
       this.peer = peer;
       this.threadPool = threadPool;
     }
     
     public ChannelPipeline getPipeline() throws Exception {
         // Create a default pipeline implementation.
         ChannelPipeline pipeline = pipeline();
          
         // Uncomment the following line if you want HTTPS
         //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
         //engine.setUseClientMode(false);
         //pipeline.addLast("ssl", new SslHandler(engine));
 
         pipeline.addLast("decoder", new HttpRequestDecoder());
         // Uncomment the following line if you don't want to handle HttpChunks.
         pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
         pipeline.addLast("encoder", new HttpResponseEncoder());
         // Remove the following line if you don't want automatic content compression.
         pipeline.addLast("deflater", new HttpContentCompressor());
         pipeline.addLast("handler", new KeeperHttpRequestHandler(peer, threadPool));
         return pipeline;
     }
}

