package io.tradle.bitkeeper;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import net.tomp2p.connection.*;
import net.tomp2p.connection.Bindings.Protocol;
import net.tomp2p.futures.*;
import net.tomp2p.p2p.*;
import net.tomp2p.peers.*;
import net.tomp2p.storage.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.socket.nio.*;

import com.google.gson.*;

/**
 * An HTTP server that serves key value pares to and from DHT.
 * 
 */
public class KeeperServer {
	private static final Random RND = new Random(System.currentTimeMillis());
	private static final String DEFAULT_CONFIG_PATH = "conf/config.json";

	public static void main(String[] args) throws Exception {
		String configPath = args.length == 0 ? DEFAULT_CONFIG_PATH : args[0];
		Config config = null;
		Gson gson = new GsonBuilder().create();
		try {
			config = gson.fromJson(new BufferedReader(new FileReader(configPath)), Config.class);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("couldn't find config file at path: " + configPath);
		}

		String storage = config.storageDir();
		if (storage != null) {
			if (storage.length() > 0) {
				File f = new File(storage);
				if (!f.exists())
					f.mkdirs();

				if (!f.isDirectory())
					throw new IllegalStateException("config parameter 'storageDir' is not a directory: " + storage);
			} 
			else
				storage = null;
		}

		Peer me = createOwnDhtPeer(config.address().dhtPort(), config.address().address(), storage);

		ExecutorService threadPool = Executors.newCachedThreadPool();

		// bootstrap to DHT network
		threadPool.execute(new DhtBoostrapper(me, config.keepers().get(0).dhtPort(), config.keepers().get(0).dhtAddress()));

		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new ServerPipelineFactory(me, threadPool));

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(config.address().httpPort()));

	}

	public static Peer createOwnDhtPeer(int myPort, String myIp,
			String storageDir) throws Exception {
		InetAddress myIpAddrress = Inet4Address.getByName(myIp);
		Bindings b = new Bindings(Protocol.IPv4, myIpAddrress, myPort, myPort);
		// b.addInterface("eth0");
		Peer client = new PeerMaker(new Number160(RND))
				.setPorts(myPort)
				.setEnableIndirectReplication(true)
				.setStorage(storageDir != null ? new StorageDisk(storageDir) : new StorageMemory()).setBindings(b)
				.makeAndListen();
		
		System.out.println("DHT client started and Listening to: " + DiscoverNetworks.discoverInterfaces(b));
		System.out.println("DHT client address visible to outside is " + client.getPeerAddress());
		return client;
	}

	static class DhtBoostrapper implements Runnable {
		int masterPort;
		InetAddress masterAddress;
		PeerAddress pa;
		Peer myself;
		boolean bootstrapped;

		DhtBoostrapper(Peer myself, int masterPort, String masterIp) throws Exception {
			masterAddress = Inet4Address.getByName(masterIp);
			this.masterPort = masterPort;
			pa = new PeerAddress(Number160.ZERO, masterAddress, masterPort, masterPort);
			this.myself = myself;
		}

		public void run() {
			while (true) {
				if (!bootstrapped) {
					// Future Bootstrap - slave
					FutureBootstrap futureBootstrap = myself.bootstrap().setInetAddress(masterAddress).setPorts(masterPort).start();
					futureBootstrap.awaitUninterruptibly();
					if (futureBootstrap.isSuccess()) {
						System.out.println("succeded to bootstrap to DHC");
						bootstrapped = true;
					} else {
						System.out.println("failed " + futureBootstrap.getFailedReason());
						bootstrapped = false;
					}
				} else {
					// ping
					FutureChannelCreator fcc = myself.getConnectionBean().getConnectionReservation().reserve(1);
					fcc.awaitUninterruptibly();

					ChannelCreator cc = fcc.getChannelCreator();

					FutureResponse fr1 = myself.getHandshakeRPC().pingTCP(pa, cc);
					fr1.awaitUninterruptibly();

					if (fr1.isSuccess()) {
						// System.out.println("peer online T:" + pa);
					} else {
						System.out.println("peer offline " + pa);
						bootstrapped = false;
					}
					// FutureResponse fr2 = master.getHandshakeRPC().pingUDP(pa,
					// cc);
					// fr2.awaitUninterruptibly();

					myself.getConnectionBean().getConnectionReservation().release(cc);

					// if (fr2.isSuccess())
					// System.out.println("peer online U:" + pa);
					// else
					// System.out.println("offline " + pa);

				}
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}
}
