/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.ml.algorithms.clustering;

import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.examples.ml.parameters.NumberOfClusters;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataGatherSender;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.api.PartitionTracker;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public final class ClusteringPreCmpTask extends UserComputeTask
    implements DataGatherSender<List<Vector>> {

  /**
   * Key used in Elastic Memory to put/get the data.
   * TODO #168: we should find better place to put this
   */
  public static final String KEY_POINTS = "points";

  /**
   * Number of clusters.
   */
  private final int numberOfClusters;

  /**
   * Points read from input data to work on.
   */
  private List<Vector> points = null;

  /**
   * Sampled points.
   */
  private List<Vector> samples = new LinkedList<>();
  private final DataParser<List<Vector>> dataParser;

  /**
   * Memory storage to put/get the data.
   */
  private final MemoryStore memoryStore;

  /**
   * Partition tracker to register partitions to.
   */
  private final PartitionTracker partitionTracker;

  /**
   * Data identifier factory to generate id for data.
   */
  private final DataIdFactory<Long> dataIdFactory;

  @Inject
  public ClusteringPreCmpTask(
      final DataParser<List<Vector>> dataParser,
      final MemoryStore memoryStore,
      final PartitionTracker partitionTracker,
      final DataIdFactory<Long> dataIdFactory,
      @Parameter(NumberOfClusters.class) final int numberOfClusters) {
    this.dataParser = dataParser;
    this.memoryStore = memoryStore;
    this.partitionTracker = partitionTracker;
    this.dataIdFactory = dataIdFactory;
    this.numberOfClusters = numberOfClusters;
  }

  @Override
  public void initialize() throws ParseException {
    points = dataParser.get();
    try {
      final List<Long> ids = dataIdFactory.getIds(points.size());
      partitionTracker.registerPartition(KEY_POINTS, ids.get(0), ids.get(ids.size() - 1));
      memoryStore.getElasticStore().putList(KEY_POINTS, ids, points);
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(final int iteration) {

    //randomly sample points so that the number of points are equal to that of clusters
    samples = sample(points, numberOfClusters);
  }

  @Override
  public List<Vector> sendGatherData(final int iteration) {
    return samples;
  }

  /**
   * Random Sampling.
   * @param points
   * @param maxNumOfSamples
   * @return
   */
  static List<Vector> sample(final List<Vector> points, final int maxNumOfSamples) {
    final List<Vector> samples = new LinkedList<>();

    if (points.isEmpty()) {
      return samples;
    }

    final Vector[] pointArray = points.toArray(new Vector[0]);
    final Random random = new Random();
    final int numberOfPoints = points.size();
    final int numberOfSamples = Math.min(maxNumOfSamples, numberOfPoints);

    for (int i = 0; i < numberOfSamples; i++) {
      final int index = random.nextInt(numberOfPoints - 1 - i);
      samples.add(pointArray[index]);
      pointArray[index] = pointArray[numberOfPoints - 1 - i];
    }

    return samples;
  }
}
