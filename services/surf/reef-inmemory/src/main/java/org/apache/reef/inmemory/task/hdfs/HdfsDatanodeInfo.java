package org.apache.reef.inmemory.task.hdfs;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Information needed for Task to establish a channel with the HDFS Datanode.
 * @see org.apache.hadoop.hdfs.protocol.DatanodeInfo
 */
public final class HdfsDatanodeInfo implements Serializable {

  private final String ipAddr;       // IP address
  private final String hostName;     // hostname claimed by datanode
  private final String datanodeUuid; // Uuid of datanode
  private final int xferPort;        // data streaming port
  private final int infoPort;        // info server port
  private final int infoSecurePort;  // info server port
  private final int ipcPort;         // IPC server port

  public HdfsDatanodeInfo(final String ipAddr,
                          final String hostName,
                          final String datanodeUuid,
                          final int xferPort,
                          final int infoPort,
                          final int infoSecurePort,
                          final int ipcPort) {
    this.ipAddr = ipAddr;
    this.hostName = hostName;
    this.datanodeUuid = datanodeUuid;
    this.xferPort = xferPort;
    this.infoPort = infoPort;
    this.infoSecurePort = infoSecurePort;
    this.ipcPort = ipcPort;
  }

  public static HdfsDatanodeInfo copyDatanodeInfo(DatanodeInfo info) {
    return new HdfsDatanodeInfo(
            info.getIpAddr(),
            info.getHostName(),
            info.getDatanodeUuid(),
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

  public String getDatanodeUuid() {
    return datanodeUuid;
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
