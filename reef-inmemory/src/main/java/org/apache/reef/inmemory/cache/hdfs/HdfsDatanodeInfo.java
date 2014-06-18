package org.apache.reef.inmemory.cache.hdfs;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class HdfsDatanodeInfo implements Serializable {

  private final String ipAddr;       // IP address
  private final String hostName;     // hostname claimed by datanode
  private final String peerHostName; // hostname from the actual connection
  private final String storageID;    // unique per cluster storageID
  private final int xferPort;        // data streaming port
  private final int infoPort;        // info server port
  private final int infoSecurePort;  // info server port
  private final int ipcPort;         // IPC server port

  public HdfsDatanodeInfo(final String ipAddr,
                          final String hostName,
                          final String peerHostName,
                          final String storageID,
                          final int xferPort,
                          final int infoPort,
                          final int infoSecurePort,
                          final int ipcPort) {
    this.ipAddr = ipAddr;
    this.hostName = hostName;
    this.peerHostName = peerHostName;
    this.storageID = storageID;
    this.xferPort = xferPort;
    this.infoPort = infoPort;
    this.infoSecurePort = infoSecurePort;
    this.ipcPort = ipcPort;
  }

  public static HdfsDatanodeInfo copyDatanodeInfo(DatanodeInfo info) {
    return new HdfsDatanodeInfo(
            info.getIpAddr(),
            info.getHostName(),
            info.getPeerHostName(),
            info.getStorageID(),
            info.getXferPort(),
            info.getInfoPort(),
            info.getInfoSecurePort(),
            info.getIpcPort()
    );
  }

  public static List<HdfsDatanodeInfo> copyDatanodeInfos(DatanodeInfo[] infos) {
    List<HdfsDatanodeInfo> infosOut = new ArrayList<>(infos.length);
    for (DatanodeInfo info : infos) {
      infosOut.add(copyDatanodeInfo(info));
    }
    return infosOut;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public String getHostName() {
    return hostName;
  }

  public String getPeerHostName() {
    return peerHostName;
  }

  public String getStorageID() {
    return storageID;
  }

  public int getXferPort() {
    return xferPort;
  }

  public int getInfoPort() {
    return infoPort;
  }

  public int getInfoSecurePort() {
    return infoSecurePort;
  }

  public int getIpcPort() {
    return ipcPort;
  }
}
