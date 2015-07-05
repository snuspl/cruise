package org.apache.reef.inmemory.driver.locality;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class YarnLocationSorter implements LocationSorter {
  private static final Logger LOG = Logger.getLogger(YarnLocationSorter.class.getName());

  /**
   * Location Sorter that makes use of Yarn's configured topology resolution
   * @param conf Yarn configuration with topology resolution
   */
  @Inject
  public YarnLocationSorter(final YarnConfiguration conf) {
    debug(conf);
    RackResolver.init(conf);
  }

  private void debug(final Configuration conf) {
    LOG.log(Level.FINE, "net.topology.node.switch.mapping.impl: {0}",
            conf.get("net.topology.node.switch.mapping.impl"));
    LOG.log(Level.FINE, "net.topology.table.file.name: {0}",
            conf.get("net.topology.table.file.name"));
  }

  @Override
  public FileMeta sortMeta(final FileMeta original, final String clientHostname) {
    if (original.getBlocksSize() == 0) {
      return original;
    }

    final FileMeta sorted = original.deepCopy();
    final Node clientNode = RackResolver.resolve(clientHostname);
    final String clientRack = clientNode.getNetworkLocation();

    for (final BlockMeta block : sorted.getBlocks()) {
      final List<NodeInfo> nodeLocal = new ArrayList<>();
      final List<NodeInfo> rackLocal = new ArrayList<>();
      final List<NodeInfo> offRack = new ArrayList<>();

      final List<NodeInfo> locations = block.getLocations();
      for (final NodeInfo location : locations) {
        if (isNodeLocal(clientHostname, location.getAddress())) {
          nodeLocal.add(location);
        } else if (isRackLocal(clientRack, location.getRack())) {
          rackLocal.add(location);
        } else {
          offRack.add(location);
        }
      }

      locations.clear();
      for (final List<NodeInfo> list : new List[] {nodeLocal, rackLocal, offRack}) {
        Collections.shuffle(list);
        locations.addAll(list);
      }
    }
    return sorted;
  }

  private boolean isNodeLocal(final String clientHostname, final String locationAddress) {
    final String locationHostname = HostAndPort.fromString(locationAddress).getHostText();
    LOG.log(Level.FINE, "isNodeLocal: {0}, {1}", new String[]{clientHostname, locationHostname});

    if (clientHostname.equals(locationHostname)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isRackLocal(final String clientRack, final String locationRack) {
    LOG.log(Level.FINE, "isRackLocal: {0}, {1}", new String[]{clientRack, locationRack});

    if (clientRack.equals(locationRack)) {
      return true;
    } else {
      return false;
    }
  }
}
