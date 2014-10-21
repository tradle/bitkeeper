package io.tradle.bitkeeper;

import java.util.List;
public class Config {
  private AddressConfig address;
  private String storageDir;
  private List<PeerConfig> dhtPeerAddresses;
 
  public AddressConfig address() {
    return address;
  }
  public String storageDir() {
    return storageDir;
  }
  public List<PeerConfig> keepers() {
    return dhtPeerAddresses;
  }
   
  public static class AddressConfig {
    private String host;
    private int    httpPort;
    private int    dhtPort;
    
    public String address() {
      return host;
    }
    public int httpPort() {
      return httpPort;
    }
    public int dhtPort() {
      return dhtPort;
    }
  }
  
  public static class PeerConfig {
    private String dhtHost;
    private int    dhtPort;
    
    public String dhtAddress() {
      return dhtHost;
    }
    public int dhtPort() {
      return dhtPort;
    }
  }
}
