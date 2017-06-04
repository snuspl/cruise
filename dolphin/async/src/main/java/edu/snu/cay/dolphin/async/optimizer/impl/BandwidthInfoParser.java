package edu.snu.cay.dolphin.async.optimizer.impl;

import edu.snu.cay.common.dataloader.HdfsSplitFetcher;
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.common.param.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yunseong on 6/4/17.
 */
final class BandwidthInfoParser {
  private final String hostnameToBandwidthFilePath;

  @Inject
  private BandwidthInfoParser(@Parameter(Parameters.HostToBandwidthFilePath.class) final String hostBandwidthFilePath) {
   this.hostnameToBandwidthFilePath = hostBandwidthFilePath;
  }

  /**
   * @return the mapping between the hostname and bandwidth of machines
   */
  private Map<String, Double> parseBandwidthInfo() {
    if (hostnameToBandwidthFilePath.equals(Parameters.HostToBandwidthFilePath.NONE)) {
      return Collections.emptyMap();
    }

    final Map<String, Double> mapping = new HashMap<>();

    final HdfsSplitInfo[] infoArr =
        HdfsSplitManager.getSplits(hostnameToBandwidthFilePath, TextInputFormat.class.getName(), 1);

    assert infoArr.length == 1; // infoArr's length is always 1(NUM_SPLIT == 1).
    final HdfsSplitInfo info = infoArr[0];
    try {
      final Iterator<Pair<LongWritable, Text>> iterator = HdfsSplitFetcher.fetchData(info);
      while (iterator.hasNext()) {
        final String text = iterator.next().getValue().toString().trim();
        if (!text.startsWith("#") && text.length() != 0) { // comments and empty lines
          final String[] split = text.split("\\s+");
          assert split.length == 2;
          final String hostname = split[0];
          final double bandwidth = Double.parseDouble(split[1]);
          mapping.put(hostname, bandwidth);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return mapping;
  }
}
