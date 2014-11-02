package org.apache.reef.inmemory.driver.locality;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test YarnLocationSorter, using a static topology table (resources/net.topology.table.txt)
 * The topology consists of:
 *   /rack0: host01, host02, host03, host04
 *   /rack1: host11, host12, host13
 *   /rack2: host21, host22, host23
 *   /rack3: host31
 * The test sorts a fileMeta with a subset of these block locations:
 *   /rack0: host01, host02, host03
 *   /rack1: host11, host12, host13
 *   /rack2: host21, host22, host23
 */
public final class YarnLocationSorterTest {

  public Map<String, String> networkMapping;

  YarnLocationSorter yarnLocationSorter;
  FileMeta fileMeta;

  /**
   * Setup tests by adding first 9 entries (3 racks, 3 hosts per rack) from
   * topology file to fileMeta.
   */
  @Before
  public void setUp() throws IOException {
    final URL url = this.getClass().getResource("/net.topology.table.txt");
    final YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
            url.getPath());
    yarnLocationSorter = new YarnLocationSorter(conf);

    final File tableMapping = new File(url.getFile());
    final BufferedReader br = new BufferedReader(new FileReader(tableMapping));

    networkMapping = new HashMap<>();

    final BlockInfo blockInfo = new BlockInfo();
    String line = null;
    int count = 0;
    while ((line = br.readLine()) != null) {
      final String[] parts =line.split("\\s+");
      networkMapping.put(parts[0], parts[1]);
      if (count < 9) { // Only add first 9 entries (3 racks, 3 hosts per rack)
        blockInfo.addToLocations(new NodeInfo(parts[0], parts[1]));
      }
      count++;
    }
    br.close();

    fileMeta = new FileMeta();
    fileMeta.setBlocks(Collections.singletonList(blockInfo));
  }

  /**
   * Test that each location in fileMeta returns:
   * 1 node local, 2 rack local, 6 off-rack locations
   */
  @Test
  public void testSortMetaNodeLocal() {
    for (final NodeInfo nodeInfo : fileMeta.getBlocks().get(0).getLocations()) {
      final String host = nodeInfo.getAddress();
      final FileMeta sorted = yarnLocationSorter.sortMeta(fileMeta, host);
      final List<NodeInfo> locations = sorted.getBlocks().get(0).getLocations();

      // Node local
      assertEquals(host, locations.get(0).getAddress());

      // Rack local
      for (int i = 1; i < 3; i++) {
        assertNotEquals(host, locations.get(i).getAddress());
        assertEquals(networkMapping.get(host), locations.get(i).getRack());
      }

      // Off-rack
      for (int i = 3; i < 9; i++) {
        assertNotEquals(host, locations.get(i).getAddress());
        assertNotEquals(networkMapping.get(host), locations.get(i).getRack());
      }
    }
  }

  /**
   * Test that /rack0/host04 (host04 is not a part of fileMeta) returns:
   * 3 rack local, 6 off-rack locations
   */
  @Test
  public void testSortMetaRackLocal() {
    final String host = "host04";

    final FileMeta sorted = yarnLocationSorter.sortMeta(fileMeta, host);
    final List<NodeInfo> locations = sorted.getBlocks().get(0).getLocations();

    // Rack local
    for (int i = 0; i < 3; i++) {
      assertNotEquals(host, locations.get(i).getAddress());
      assertEquals(networkMapping.get(host), locations.get(i).getRack());
    }

    // Off-rack
    for (int i = 3; i < 9; i++) {
      assertNotEquals(host, locations.get(i).getAddress());
      assertNotEquals(networkMapping.get(host), locations.get(i).getRack());
    }
  }

  /**
   * Test that the /rack4/host04 (no hosts in rack04 are a part of fileMeta) returns:
   * 9 off-rack locations
   */
  @Test
  public void testSortMetaOffRack() {
    final String host = "host31";

    final FileMeta sorted = yarnLocationSorter.sortMeta(fileMeta, host);
    final List<NodeInfo> locations = sorted.getBlocks().get(0).getLocations();

    // Off-rack
    for (int i = 0; i < 9; i++) {
      assertNotEquals(host, locations.get(i).getAddress());
      assertNotEquals(networkMapping.get(host), locations.get(i).getRack());
    }
  }
}
