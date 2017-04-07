/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.common.dataloader;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.io.IOException;
import java.util.*;

/**
 * This class overrides {@link #getSplits(JobConf, int)} of {@link FileInputFormat}
 * to break the dependency between the number of HDFS blocks and the number of {@link InputSplit}s.
 * It splits (if it's splittable) HDFS blocks ignoring block size and alignment.
 *
 * So it guarantees {@link #getSplits(JobConf, int)} to return the splits as requested numbers
 * except following exceptional cases:
 * 1) when the number of total line in a file is smaller than the number of requested splits.
 * 2) when a file format is not splitable (see {@link #isSplitable}, which can be overridden).
 * 3) when a file is not specified (will return one empty split).
 *
 * For simplicity, this class restricts only one file to be handled at once.
 * We may extend it to support loading multiple files with some policy.
 *
 * Most of code is resembled from {@link FileInputFormat} and
 * a code deciding split size (see line 65) is modified and so subsequent codes.
 */
public abstract class ExactNumSplitFileInputFormat<K, V> extends FileInputFormat<K, V> {

  // if remaining bytes in a file is smaller than GOAL_SPLIT_SIZE * SPLIT_SLOP,
  // makes them as one split.
  private static final double SPLIT_SLOP = 1.1; // 10% slop

  @Override
  public InputSplit[] getSplits(final JobConf job, final int numSplits)
      throws IOException {
    final FileStatus[] files = listStatus(job);
    if (files.length > 1) {
      throw new IOException("Cannot support multiple files");
    }

    long totalSize = 0;                                  // compute total size
    for (final FileStatus file : files) {                // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: " + file.getPath());
      }
      totalSize += file.getLen();
    }
    final long splitSize = totalSize / (numSplits == 0 ? 1 : numSplits); // modified

    // generate splits
    final ArrayList<FileSplit> splits = new ArrayList<>(numSplits);
    final NetworkTopology clusterMap = new NetworkTopology();
    for (final FileStatus file : files) {
      final Path path = file.getPath();
      final long length = file.getLen();

      if (length != 0) {
        final FileSystem fs = path.getFileSystem(job);
        final BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }

        // split if possible
        if (isSplitable(fs, path)) {
          long bytesRemaining = length;
          while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            final String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length - bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            final String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          final String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
              0, length, clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else {
        // Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  /**
   * This function identifies and returns the hosts that contribute
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset
   * @param splitSize
   * @return two arrays - one of hosts that contribute most to this split, and
   *    one of hosts that contribute most to this split that have the data
   *    cached on them
   * @throws IOException
   */
  private String[][] getSplitHostsAndCachedHosts(final BlockLocation[] blkLocations,
                                                 final long offset,
                                                 final long splitSize,
                                                 final NetworkTopology clusterMap)
      throws IOException {

    final int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() +
        blkLocations[startIndex].getLength() - offset;

    // If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return new String[][] {blkLocations[startIndex].getHosts(),
          blkLocations[startIndex].getCachedHosts() };
    }

    long remainingSize = splitSize;

    final long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    remainingSize -= bytesInThisBlock;

    while (remainingSize > 0) {
      bytesInThisBlock =
          Math.min(remainingSize, blkLocations[index++].getLength());
      remainingSize -= bytesInThisBlock;
    }

    final long bytesInLastBlock = bytesInThisBlock;
    final int endIndex = index - 1;

    final Map<Node, NodeInfo> hostsMap = new IdentityHashMap<>();
    final Map<Node, NodeInfo> racksMap = new IdentityHashMap<>();
    String[] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of
    // bytes at each level. See TestGetSplitHosts.java

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      } else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      } else {
        bytesInThisBlock = blkLocations[index].getLength();
      }

      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support CentComm at different
      // levels

      for (final String topo : allTopos) {

        Node node;
        final Node parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }

        nodeInfo = hostsMap.get(node);

        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node, nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode, parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        } else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos

    } // for all indices

    // We don't yet support cached hosts when bytesInThisBlock > splitSize
    return new String[][] {identifyHosts(allTopos.length, racksMap), new String[0]};
  }


  private String[] identifyHosts(final int replicationFactor,
                                 final Map<Node, NodeInfo> racksMap) {

    final String[] retVal = new String[replicationFactor];

    final List <NodeInfo> rackList = new LinkedList<>();

    rackList.addAll(racksMap.values());

    // Sort the racks based on their contribution to this split
    sortInDescendingOrder(rackList);

    boolean done = false;
    int index = 0;

    // Get the host list for all our aggregated items, sort
    // them and return the top entries
    for (final NodeInfo ni : rackList) {

      final Set<NodeInfo> hostSet = ni.getLeaves();

      final List<NodeInfo>hostList = new LinkedList<>();
      hostList.addAll(hostSet);

      // Sort the hosts in this rack based on their contribution
      sortInDescendingOrder(hostList);

      for (final NodeInfo host : hostList) {
        // Strip out the port number from the host name
        retVal[index++] = host.node.getName().split(":")[0];
        if (index == replicationFactor) {
          done = true;
          break;
        }
      }

      if (done) {
        break;
      }
    }
    return retVal;
  }

  private void sortInDescendingOrder(final List<NodeInfo> mylist) {
    Collections.sort(mylist, (obj1, obj2) -> {
      if (obj1 == null || obj2 == null) {
        return -1;
      }

      if (obj1.getValue() == obj2.getValue()) {
        return 0;
      } else {
        return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
      }
    }
    );
  }

  private String[] fakeRacks(final BlockLocation[] blkLocations, final int index)
      throws IOException {
    final String[] allHosts = blkLocations[index].getHosts();
    final String[] allTopos = new String[allHosts.length];
    for (int i = 0; i < allHosts.length; i++) {
      allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
    }
    return allTopos;
  }

  private static class NodeInfo {
    private final Node node;
    private final Set<Integer> blockIds;
    private final Set<NodeInfo> leaves;

    private long value;

    NodeInfo(final Node node) {
      this.node = node;
      this.blockIds = new HashSet<>();
      this.leaves = new HashSet<>();
    }

    long getValue() {
      return value;
    }

    void addValue(final int blockIndex, final long deltaValue) {
      if (blockIds.add(blockIndex)) {
        this.value += deltaValue;
      }
    }

    Set<NodeInfo> getLeaves() {
      return leaves;
    }

    void addLeaf(final NodeInfo nodeInfo) {
      leaves.add(nodeInfo);
    }
  }
}
