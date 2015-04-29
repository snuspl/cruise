/**
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
package edu.snu.reef.flexion.examples.ml.algorithms.clustering;

import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.ParseException;
import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.ml.parameters.NumberOfClusters;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherSender;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public final class ClusteringPreCmpTask extends UserComputeTask
    implements DataGatherSender<List<Vector>> {

  /**
   * Number of clusters
   */
  private final int numberOfClusters;

  /**
   * Points read from input data to work on
   */
  private List<Vector> points = null;

  /**
   * Sampled points
   */
  private List<Vector> samples = new LinkedList<>();
  private final DataParser<List<Vector>> dataParser;

  /**
   * @param dataParser
   * @param numberOfClusters
   */
  @Inject
  public ClusteringPreCmpTask(
      final DataParser<List<Vector>> dataParser,
      @Parameter(NumberOfClusters.class) final int numberOfClusters) {
    this.dataParser = dataParser;
    this.numberOfClusters = numberOfClusters;
  }

  @Override
  public void initialize() throws ParseException {
    points = dataParser.get();
  }

  @Override
  public void run(int iteration) {

    //randomly sample points so that the number of points are equal to that of clusters
    samples = sample(points, numberOfClusters);
  }

  @Override
  public List<Vector> sendGatherData(int iteration) {
    return samples;
  }

  /**
   * Random Sampling
   * @param points
   * @param maxNumOfSamples
   * @return
   */
  static List<Vector> sample(List<Vector> points, int maxNumOfSamples) {
    final List<Vector> samples = new LinkedList<>();

    if (points.isEmpty()) {
      return samples;
    }

    final Vector[] pointArray = points.toArray(new Vector[0]);
    final Random random = new Random();
    final int numberOfPoints = points.size();
    final int numberOfSamples = Math.min(maxNumOfSamples, numberOfPoints);

    for (int i=0; i<numberOfSamples; i++) {
      final int index = random.nextInt(numberOfPoints - 1 - i);
      samples.add(pointArray[index]);
      pointArray[index] = pointArray[numberOfPoints-1-i];
    }

    return samples;
  }
}