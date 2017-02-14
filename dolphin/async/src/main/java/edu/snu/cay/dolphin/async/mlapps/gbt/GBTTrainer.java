/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.param.Parameters.Iterations;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.TrainingDataProvider;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.utils.Tuple3;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.mlapps.gbt.GBTParameters.*;

/**
 * {@link Trainer} class for the GBTREEF application.
 * Tree growing algorithm and boosting algorithm follows XGBoost.
 */
final class GBTTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(GBTTrainer.class.getName());

  private static final int TYPE_LINE = -1;
  private static final int LABEL_SIZE_THRESHOLD = 10;

  /**
   * Max iteration of run() method.
   */
  private final int maxIteration;

  private final ParameterWorker<Integer, List<Vector>, List<Vector>> parameterWorker;

  private final int numFeatures;
  private final int numData;

  /**
   * Do not do full optimization in each step.
   * By optimizing only for stepSize, over-fitting can be prevented(usually set in 0.1).
   */
  private double stepSize;

  /**
   * lambda and gamma are regularization constants.
   * lambda is related to leaf, and gamma is related to total tree size.
   */
  private final double lambda;
  private final double gamma;

  /**
   * residual = (real y-value) - (predicted y-value).
   * Predicted y-value is calculated by using all the trees that are built.
   */
  private final Vector residual;

  private final TrainingDataProvider<Long, GBTData> trainingDataProvider;

  private final VectorFactory vectorFactory;

  /**
   * Sort GBT data for each feature values and store it in sortedByFeature list.
   * It is pre-calculated before the optimizing process is proceeded.
   */
  private final List<SortedTree> sortedByFeature;

  /**
   * For each feature, pre-count the number and pre-calculate g-values' sum of data that are belong to the each label.
   * groupedByLabel.get(i) : for the i-th feature, list of paired values.
   */
  private final List<GroupedTree> groupedByLabel;

  /**
   * For each feature, if the feature is label type, store each data's label for that feature.
   * labelList.get(i) : for the i-th feature, list of each data's label.
   */
  private final List<List<Integer>> labelList;

  /**
   * keyGPair stores set of Pair(identity, g-value) to the i-th data in this worker.
   */
  private final List<Pair<Integer, Double>> keyGPair;

  /**
   * If featureType.get(i) == 0, i-th feature type is real number.
   * If featureType.get(i) == 1, i-th feature type is label.
   */
  private final List<Integer> featureType;

  /**
   * If valueType == 0, y-value type is a real number.
   * If valueType != 0, y-value type is a label. There are valueType-number of labels for the y value.
   * ex) if valueType == 5, five types of label exist for y-value.
   */
  private int valueType;

  /**
   * tree list is the form of CART(Classification and Regression Tree).
   *
   * Tree's node index looks as follow :
   *          0
   *         / \
   *        1   2
   *       / \ / \
   *      3  4 5 6
   *      .........
   *
   * First instance is the number of criterion feature to divide the node and the second instance is the split point.
   * ex) (3,7) means if the feature3 value is smaller than 7, go to left child. Else, go to right child.
   *
   * If the criterion feature is in the form of real number, split point is real number.
   *  - If the feature value is less than or equal to the split point, the data goes to the left child.
   *  - If the feature value is greater than the split point, the data goes to the right child.)
   * If the criterion feature is in the form of labels, split point is binary number.
   *  - Label order is descending order from the higher digit number.
   *  - 0 indicates left child while 1 indicates right child.
   * ex) 10011 : if the feature's label is 2, 3, the data goes to left child.
   *             if the feature's label is 0, 1, 4, the data goes to the right child.
   *
   * If the first instance is NodeState.EMPTY(-2), it means the tree node does not exist.
   * If the first instance is NodeState.LEAF(-1), it means the tree node is a leaf node.
   */
  private final GBTree gbTree;

  /**
   * i-th list of dataTree includes set of data numbers that are in the i-th tree node.
   */
  private final DataTree dataTree;

  /**
   * Maximum depth of the tree(for regularization).
   */
  private final int treeMaxDepth;

  /**
   * Size of the tree.
   */
  private final int treeSize;

  /**
   * Minimum size of leaf(for regularization).
   */
  private final int leafMinSize;

  /**
   * When pull all the trees from the server, save those trees in this forest.
   */
  private final List<GBTree> forest;

  @Inject
  private GBTTrainer(final ParameterWorker<Integer, List<Vector>, List<Vector>> parameterWorker,
                     @Parameter(NumFeatures.class) final int numFeatures,
                     @Parameter(NumData.class) final int numData,
                     @Parameter(StepSize.class) final double stepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(Gamma.class) final double gamma,
                     @Parameter(TreeMaxDepth.class) final int treeMaxDepth,
                     @Parameter(LeafMinSize.class) final int leafMinSize,
                     @Parameter(Iterations.class) final int iterations,
                     final TrainingDataProvider<Long, GBTData> trainingDataProvider,
                     final VectorFactory vectorFactory) {
    this.parameterWorker = parameterWorker;
    this.numFeatures = numFeatures;
    this.numData = numData;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.gamma = gamma;
    this.treeMaxDepth = treeMaxDepth;
    this.treeSize = (1 << treeMaxDepth) - 1;
    this.leafMinSize = leafMinSize;
    this.trainingDataProvider = trainingDataProvider;
    this.vectorFactory = vectorFactory;
    this.residual = vectorFactory.createDenseZeros(numData);
    this.sortedByFeature = new ArrayList<>(numFeatures);
    this.groupedByLabel = new ArrayList<>(numFeatures);
    this.labelList = new ArrayList<>();
    this.keyGPair = new ArrayList<>();
    this.featureType = new ArrayList<>();
    this.gbTree = new GBTree(treeMaxDepth);
    this.dataTree = new DataTree(treeMaxDepth);
    this.forest = new ArrayList<>();
    this.maxIteration = iterations;
  }

  @Override
  public void initialize() {
    // If there is a feature type line in this worker's data set, push the feature type to the server.
    trainingDataProvider.prepareDataForEpoch();
    final Map<Long, GBTData> nextTrainingData = trainingDataProvider.getNextTrainingData();
    final List<GBTData> instances = new ArrayList<>(nextTrainingData.values());
    for (final GBTData instance : instances) {
      if (instance.getIdentity() == TYPE_LINE) {
        for (int i = 0; i < numFeatures; i++) {
          featureType.add((int)instance.getFeature().get(i));
        }
        valueType = (int)instance.getValue();
        pushType();
        break;
      }
    }
  }

  @Override
  public void cleanup() {
  }

  /**
   * Following is the total procedure :
   * 1) Pull all the trees that is up-to-date and calculate residual values.
   * 2) Pre-process sorting and grouping.
   *    - if the feature type is real number, sort GBTData.
   *    - if the feature type is label, pre-calculate sum of g values for each class.
   * 3) Build tree using pre-processed data.
   * 4) Push the tree to the server.
   *
   * @param iteration the index of current iteration
   */
  @Override
  public void run(final int iteration, final AtomicBoolean abortFlag) {
    final long startTime = System.currentTimeMillis();

    // At this part, you can get all the training data.
    // You have to set the mini-batch size larger than the data size.
    final Map<Long, GBTData> nextTrainingData = trainingDataProvider.getNextTrainingData();
    final List<GBTData> instances = new ArrayList<>(nextTrainingData.values());

    // Pull the feature type vector(real number type : 0, label type : 1).
    pullTypeVectors();

    // Divide into two cases : Regression / Classification
    if (valueType == 0) {
      // Calculate residual values for each data in this worker using all the trees.
      calculateResidual(instances, 0);

      // Get data from instances list and fill the certain lists(keyGPair, sortedByFeature, labelList).
      initializePreProcessingLists();
      final int instanceSize = settingPreProcessingLists(instances);

      // Pre-processing (prepare two lists to build tree):
      // For each feature type, pre-process the data for the following rules:
      //    - if the feature type is real number, sort training data depending on the feature values(sortedByFeature).
      //    - if the feature type is label type, group the data by its label and then calculate its g-values' sum and
      //      count the number of data in each label(listLabel, groupedByLabel).
      preProcessLists();

      // Build tree model using preprocessed data.
      LOG.log(Level.INFO, "Before building tree: {0}", System.currentTimeMillis() - startTime);
      buildTree(instanceSize);
      LOG.log(Level.INFO, "After building tree: {0}", System.currentTimeMillis() - startTime);

      // Push a tree that is built in this run iteration to the server.
      pushTree(0);

      // Clean up data that must be cleared before the next run iteration.
      cleanAllDataForNextRun();
      calculateResidual(instances, 0);
      LOG.log(Level.INFO, "loss value : {0}", computeLoss(instances));
    } else {
      for (int label = 0; label < valueType; label++) {
        // Calculate residual values for each data in this worker using all the trees for this label.
        calculateResidual(instances, label);

        // Get data from instances list and fill the certain lists(keyGPair, sortedByFeature, labelList).
        initializePreProcessingLists();
        final int instanceSize = settingPreProcessingLists(instances);

        // Pre-processing (prepare two lists to build tree):
        // For each feature type, pre-process the data for the following rules:
        //    - if the feature type is real number, sort training data depending on the feature values(sortedByFeature).
        //    - if the feature type is label type, group the data by its label and then calculate its g-values' sum and
        //      count the number of data in each label(listLabel, groupedByLabel).
        preProcessLists();

        // Build tree model using preprocessed data.
        LOG.log(Level.INFO, "Before building tree: {0}", System.currentTimeMillis() - startTime);
        buildTree(instanceSize);
        LOG.log(Level.INFO, "After building tree: {0}", System.currentTimeMillis() - startTime);

        // Push a tree that is built in this run iteration to the server.
        pushTree(label);

        // Clean up data that must be cleared before the next run iteration.
        cleanAllDataForNextRun();
      }
      double loss = 0;
      for (int label = 0; label < valueType; label++) {
        calculateResidual(instances, label);
        loss += computeLoss(instances);
      }
      LOG.log(Level.INFO, "loss value : {0}", loss);
    }
    // This is for the test.
    if (iteration == (maxIteration - 1)) {
      testTrainingCode(instances);
    }
    LOG.log(Level.INFO, "running time : {0}", System.currentTimeMillis() - startTime);
  }

  /**
   * Tree building procedure which minimizing object function.
   * For each tree node, find it's best split point and split the node.
   * If the node is inappropriate to split(by constraint such as max tree depth or min leaf size),
   * change the node to a leaf.
   */
  private void buildTree(final int dataSize) {
    for (int i = 0; i < treeSize; i++) {
      dataTree.add(new ArrayList<>());
    }
    for (int i = 0; i < dataSize; i++) {
      dataTree.get(0).add(i);
    }
    int depth = 1;
    int depthIncreasingCheck = 2;
    for (int treeNode = 0; treeNode < treeSize; treeNode++) {
      if (treeNode > depthIncreasingCheck - 2) {
        depth += 1;
        depthIncreasingCheck *= 2;
      }
      final int nodeSize = dataTree.get(treeNode).size();

      // if nodeSize == 0, node does not exist.
      if (nodeSize == 0) {
        gbTree.add(Pair.of(NodeState.EMPTY.getValue(), 0.0));
        continue;
      }

      // if nodeSize is smaller than leafMinSize or depth is deeper than treeMaxDepth, make the node to a leaf node.
      if (nodeSize <= leafMinSize || depth >= treeMaxDepth) {
        makeLeaf(treeNode);
        continue;
      }

      // Split this node by the best choice of a feature and a value.
      bestSplit(treeNode, dataSize);
    }
  }

  /**
   * Find the best split of the chosen node and split it.
   * You can find the best split by calculating Gain values for each feature and split value, and comparing each other.
   * If the maximum Gain value is negative, make the node to a leaf.
   *
   * @param treeNode : a node which we want to find the best split.
   */
  private void bestSplit(final int treeNode, final int dataSize) {
    double gSum = 0;
    double bestGain = -gamma;
    int bestFeature = 0;
    double bestSplitValue = 0;
    final List<Integer> thisNode = dataTree.get(treeNode);
    final int nodeSize = thisNode.size();
    for (final int nodeMember : thisNode) {
      gSum += keyGPair.get(nodeMember).getRight();
    }
    final double totalGain = gSum * gSum / (2 * nodeSize + lambda) + gamma;

    // For each feature, find the best split point.
    // If the best split point in certain feature splits better than global best split point, then save the condition.
    for (int feature = 0; feature < numFeatures; feature++) {
      if (featureType.get(feature) == 0) {
        final Tuple3<Double, Integer, Double> bestChoice = bestSplitRealNumberFeature(treeNode, feature, gSum,
                                                           totalGain, bestGain, bestFeature, bestSplitValue);
        bestGain = bestChoice.getFirst();
        bestFeature = bestChoice.getSecond();
        bestSplitValue = bestChoice.getThird();
      } else {
        final Tuple3<Double, Integer, Double> bestChoice = bestSplitLabelFeature(treeNode, feature, gSum, totalGain,
                                                           bestGain, bestFeature, bestSplitValue);
        bestGain = bestChoice.getFirst();
        bestFeature = bestChoice.getSecond();
        bestSplitValue = bestChoice.getThird();
      }
    }

    // After choosing the best split condition(in the upper for loop), split the data in this node into left
    // child and right child with the condition.
    // When dividing the data(using dataTree), split the pre-processed data together for the child nodes' fast and
    // convenient calculation.
    // If the bestGain <= 0, then stop splitting the node and make the node to leaf.
    // Position array saves the position of each data, whether each data goes to left child(0) or right child(1).
    final int[] position = new int[dataSize];
    if (bestGain > 0) {
      gbTree.add(Pair.of(bestFeature, bestSplitValue));
      splitActualDataAndSetPosition(treeNode, bestFeature, bestSplitValue, position);
      splitPreprocessedData(treeNode, position);
    } else {
      makeLeaf(treeNode);
    }

    // For the memory efficiency, clear thisNode since the node list will not be used anymore.
    thisNode.clear();
  }

  /**
   * Split the treeNode with the given feature(feature type is real number).
   * Initial condition : Empty left child / All the members in dataTree(treeNode) are in right child.
   * As passing each data in the right child one by one to the left child, calculate the tempGain.
   * If the tempGain is larger than the global gain(in here, retGain), change the condition for the best condition.
   * Left child and right child must have at least one member(they must not be empty).
   */
  private Tuple3<Double, Integer, Double> bestSplitRealNumberFeature(final int treeNode, final int feature,
                                                                     final double gSum, final double totalGain,
                                                                     final double bestGain, final int bestFeature,
                                                                     final double bestSplitValue) {
    final int nodeSize = dataTree.get(treeNode).size();
    double retGain = bestGain;
    int retFeature = bestFeature;
    double retSplitValue = bestSplitValue;
    double gL = 0;
    final List<Pair<Integer, Double>> thisSortedFeature = sortedByFeature.get(feature).get(treeNode);

    for (int dataNum = 0; dataNum < nodeSize - 1; dataNum++) {
      boolean childNotExistError = false;
      final Pair<Integer, Double> data = thisSortedFeature.get(dataNum);
      gL += keyGPair.get(data.getLeft()).getRight();
      while (Math.abs(thisSortedFeature.get(dataNum + 1).getRight() - data.getRight()) < 1e-9) {
        gL += keyGPair.get(thisSortedFeature.get(++dataNum).getLeft()).getRight();
        if (dataNum >= nodeSize - 1) {
          childNotExistError = true; break;
        }
      }
      if (!childNotExistError) {
        final double tempGain = calculateGain(gL, gSum - gL, dataNum + 1, nodeSize - (dataNum + 1), totalGain);
        if (tempGain > retGain) {
          retGain = tempGain;
          retFeature = feature;
          retSplitValue = data.getRight();
        }
      }
    }

    return new Tuple3<>(retGain, retFeature, retSplitValue);
  }

  /**
   * Split the treeNode with the given feature(feature type is label).
   *
   * Initial Condition : All the members in dataTree(treeNode) are in left child / Empty right child.
   * For every combinations of the feature's label set, calculate the tempGain.
   * If the tempGain is larger than the global gain(in here, retGain), change the condition for the best condition.
   * To travel all the possible combinations, grey code is used.
   * Since only one feature changing occurs as iteration goes along, tempGain can be calculated efficiently.
   * Left child and right child must have at least one member(they must not be empty).
   *
   * If the number of label type is smaller than LABEL_SIZE_THRESHOLD, try all the combinations of label types.
   * Else, find the best type to pass for each iteration from left child to right child as passing each type one by one.
   */
  private Tuple3<Double, Integer, Double> bestSplitLabelFeature(final int treeNode, final int feature,
                                                                final double gSum, final double totalGain,
                                                                final double bestGain, final int bestFeature,
                                                                final double bestSplitValue) {
    final int nodeSize = dataTree.get(treeNode).size();
    double retGain = bestGain;
    int retFeature = bestFeature;
    double retSplitValue = bestSplitValue;
    double gL = gSum;
    int countLeft = nodeSize;
    final List<Pair<Integer, Double>> thisGroupedNode = groupedByLabel.get(feature).get(treeNode);
    final int numLabel = thisGroupedNode.size();
    if (thisGroupedNode.size() <= LABEL_SIZE_THRESHOLD) {
      final List<Integer> greyCode = createGreyCode(numLabel);
      for (int i = 1; i < greyCode.size(); i++) {
        int changedFeature = 0;
        int changedDigit = greyCode.get(i) ^ greyCode.get(i - 1);
        while (true) {
          if (changedDigit % 2 == 1) {
            break;
          }
          changedDigit /= 2;
          changedFeature++;
        }
        final int addOrSubtract;
        if (greyCode.get(i) > greyCode.get(i - 1)) {
          addOrSubtract = 1;
        } else {
          addOrSubtract = -1;
        }
        final Pair<Integer, Double> changedLabel = thisGroupedNode.get(changedFeature);
        countLeft -= addOrSubtract * changedLabel.getLeft();
        gL -= addOrSubtract * changedLabel.getRight();
        if (countLeft == 0 || countLeft == nodeSize) {
          continue;
        }
        final double tempGain = calculateGain(gL, gSum - gL, countLeft, nodeSize - countLeft, totalGain);
        if (tempGain > retGain) {
          retGain = tempGain;
          retFeature = feature;
          retSplitValue = greyCode.get(i).doubleValue();
        }
      }
    } else {
      double preGain = 0;
      final int[] position = new int[numLabel];
      for (int i = 0; i < numLabel; i++) {
        position[i] = 0;
      }
      for (int i = 0; i < numLabel - 1; i++) {
        double bestDiff = -1;
        int changedLabel = -1;
        for (int label = 0; label < numLabel; label++) {
          if (position[label] == 0) {
            final int changedNum = thisGroupedNode.get(label).getLeft();
            final double changedG = thisGroupedNode.get(label).getRight();
            countLeft -= changedNum;
            if (countLeft == 0 || countLeft == nodeSize) {
              countLeft += changedNum;
              continue;
            }
            gL -= changedG;
            final double tempGain = calculateGain(gL, gSum - gL, countLeft, nodeSize - countLeft, totalGain);
            final double diff = tempGain - preGain;
            if (diff > bestDiff) {
              bestDiff = diff;
              changedLabel = label;
            }
            countLeft += changedNum;
            gL += changedG;
          }
        }
        if (bestDiff > 0) {
          position[changedLabel] = 1;
          final int changedNum = thisGroupedNode.get(changedLabel).getLeft();
          final double changedG = thisGroupedNode.get(changedLabel).getRight();
          countLeft -= changedNum;
          gL -= changedG;
          preGain = calculateGain(gL, gSum - gL, countLeft, nodeSize - countLeft, totalGain);
        } else {
          break;
        }
      }
      final double tempGain = calculateGain(gL, gSum - gL, countLeft, nodeSize - countLeft, totalGain);
      if (tempGain > retGain) {
        retGain = tempGain;
        retFeature = feature;
        retSplitValue = (double)createBinary(position, numLabel);
      }
    }

    return new Tuple3<>(retGain, retFeature, retSplitValue);
  }

  /**
   * Split the actual data in dataTree and set each data's position whether to go left child(0) or right child(1).
   */
  private void splitActualDataAndSetPosition(final int treeNode, final int bestFeature, final double bestSplitValue,
                                             final int[] position) {
    final List<Integer> thisNode = dataTree.get(treeNode);
    final List<Integer> leftChild = dataTree.leftChild(treeNode);
    final List<Integer> rightChild = dataTree.rightChild(treeNode);

    if (featureType.get(bestFeature) == 0) {
      for (final Pair<Integer, Double> data : sortedByFeature.get(bestFeature).get(treeNode)) {
        if (data.getRight() <= bestSplitValue) {
          leftChild.add(data.getLeft());
          position[data.getLeft()] = 0;
        } else {
          rightChild.add(data.getLeft());
          position[data.getLeft()] = 1;
        }
      }
    } else {
      final int[] classPosition = new int[groupedByLabel.get(bestFeature).get(treeNode).size()];
      for (int i = 0; i < classPosition.length; i++) {
        classPosition[i] = 0;
      }
      int tempBestSplitValue = (int)bestSplitValue;
      for (int i = 0; i < classPosition.length; i++) {
        if (tempBestSplitValue == 0) {
          break;
        }
        if (tempBestSplitValue % 2 == 1) {
          classPosition[i] = 1;
        }
        tempBestSplitValue /= 2;
      }
      for (final int dataName : thisNode) {
        if (classPosition[labelList.get(bestFeature).get(dataName)] == 0) {
          leftChild.add(dataName);
          position[dataName] = 0;
        } else {
          rightChild.add(dataName);
          position[dataName] = 1;
        }
      }
    }
  }

  /**
   * Split the pre-processed data when the node splits into two children.
   */
  private void splitPreprocessedData(final int treeNode, final int[] position) {
    final List<Integer> thisNode = dataTree.get(treeNode);
    for (int i = 0; i < numFeatures; i++) {
      if (featureType.get(i) == 0) {
        final List<Pair<Integer, Double>> thisSortedFeature = sortedByFeature.get(i).get(treeNode);
        final List<Pair<Integer, Double>> leftChildSorted = sortedByFeature.get(i).leftChild(treeNode);
        final List<Pair<Integer, Double>> rightChildSorted = sortedByFeature.get(i).rightChild(treeNode);
        for (final Pair<Integer, Double> data : thisSortedFeature) {
          if (position[data.getLeft()] == 0) {
            leftChildSorted.add(data);
          } else {
            rightChildSorted.add(data);
          }
        }
        thisSortedFeature.clear();
      } else {
        final List<Pair<Integer, Double>> thisGroupedLabel = groupedByLabel.get(i).get(treeNode);
        final List<Pair<Integer, Double>> leftChild = groupedByLabel.get(i).leftChild(treeNode);
        final List<Pair<Integer, Double>> rightChild = groupedByLabel.get(i).rightChild(treeNode);
        for (final int data : thisNode) {
          final int label = labelList.get(i).get(data);
          final double gValue = keyGPair.get(data).getRight();
          if (position[data] == 0) {
            final int oldNum = leftChild.get(label).getLeft();
            final double oldGValue = leftChild.get(label).getRight();
            leftChild.set(label, Pair.of(oldNum + 1, oldGValue + gValue));
          } else {
            final int oldNum = rightChild.get(label).getLeft();
            final double oldGValue = rightChild.get(label).getRight();
            rightChild.set(label, Pair.of(oldNum + 1, oldGValue + gValue));
          }
        }
        thisGroupedLabel.clear();
      }
    }
  }

  /**
   * Initialize pre-processing lists(sortedByFeature, labelList, groupedByLabelG, groupedByLabelNum).
   */
  private void initializePreProcessingLists() {
    for (int i = 0; i < numFeatures; i++) {
      sortedByFeature.add(new SortedTree(treeMaxDepth));
      groupedByLabel.add(new GroupedTree(treeMaxDepth));
      labelList.add(new ArrayList<>());
    }
  }

  /**
   * Set the pre-processing lists by using instances list and return size of instances list except a data with
   * TYPE_LINE(-1) identity.
   */
  private int settingPreProcessingLists(final List<GBTData> instances) {
    int instanceSize = 0;
    for (final GBTData instance : instances) {
      final int identity = instance.getIdentity();
      final Vector featureVector = instance.getFeature();
      if (identity == TYPE_LINE) {
        continue;
      }
      keyGPair.add(Pair.of(identity, -2.0 * residual.get(identity)));
      for (int feature = 0; feature < numFeatures; feature++) {
        if (featureType.get(feature) == 0) {
          sortedByFeature.get(feature).get(0).add(Pair.of(instanceSize, featureVector.get(feature)));
        } else {
          labelList.get(feature).add((int)featureVector.get(feature));
        }
      }
      instanceSize++;
    }
    return instanceSize;
  }

  /**
   * For the root node of the tree, sort by feature values or group and calculate the needed values for each label.
   * For other nodes of the tree, make a list with 0 values(length is the same with the root node's list).
   */
  private void preProcessLists() {
    for (int feature = 0; feature < numFeatures; feature++) {
      if (featureType.get(feature) == 0) {
        sortedByFeature.get(feature).get(0).sort(FEATURE_COMPARATOR);
      } else {
        final List<Pair<Integer, Double>> groupedByLabelRoot = groupedByLabel.get(feature).get(0);
        int instance = 0;
        for (final int label : labelList.get(feature)) {
          final int existingLabelNum = groupedByLabelRoot.size();
          if (label >= existingLabelNum) {
            final int widenLength = label + 1 - existingLabelNum;
            for (int i = 0; i < widenLength; i++) {
              groupedByLabelRoot.add(Pair.of(0, 0.0));
            }
          }
          final int oldNum = groupedByLabelRoot.get(label).getLeft();
          final double oldGValue = groupedByLabelRoot.get(label).getRight();
          final double addedGValue = keyGPair.get(instance++).getRight();
          groupedByLabelRoot.set(label, Pair.of(oldNum + 1, oldGValue + addedGValue));
        }
      }
    }
    for (int feature = 0; feature < numFeatures; feature++) {
      if (featureType.get(feature) > 0) {
        final int existingLabelNum = groupedByLabel.get(feature).get(0).size();
        for (int node = 1; node < treeSize; node++) {
          for (int i = 0; i < existingLabelNum; i++) {
            groupedByLabel.get(feature).get(node).add(Pair.of(0, 0.0));
          }
        }
      }
    }
  }

  /**
   * Pull all the trees related to the label in the server side and calculate the predicted value for each data
   * in this worker.
   * Then, calculate residual values for each data by using real y-values and predicted values.
   */
  private void calculateResidual(final List<GBTData> instances, final int label) {
    pullAllTrees(label);
    final double[] predictedValue = new double[numData];
    for (int i = 0; i < numData; i++) {
      predictedValue[i] = 0;
    }
    for (final GBTree thisTree : forest) {
      for (final GBTData instance : instances) {
        if (instance.getIdentity() != TYPE_LINE) {
          predictedValue[instance.getIdentity()] += stepSize * predictByTree(instance, thisTree);
        }
      }
    }
    for (final GBTData instance : instances) {
      final int identity = instance.getIdentity();
      if (identity != TYPE_LINE) {
        final double thisPredictedValue = predictedValue[identity];
        final double thisValue = instance.getValue();
        if (valueType == 0) {
          residual.set(identity, thisValue - thisPredictedValue);
        } else {
          if (Math.abs(thisValue - label) < 1e-9) {
            residual.set(identity, 1 - thisPredictedValue);
          } else {
            residual.set(identity, -thisPredictedValue);
          }
        }
      }
    }
  }

  /**
   * Push the tree that is built in this run() iteration.
   * Since updater is only possible to update for List<Vector> form, split each tree node into two components
   * (bestFeature and bestSplitValue), and push them separately.
   */
  private void pushTree(final int label) {
    pushBestFeatures(label);
    pushBestSplitValues(label);
  }

  /**
   * Push the bestFeature list to the server.
   * bestFeature will be placed in the index-1 of parameter server.
   */
  private void pushBestFeatures(final int label) {
    final List<Vector> pushBestFeatureList = new LinkedList<>();
    pushBestFeatureList.add(vectorFactory.createDenseOnes(1));
    final Vector bestFeatures = vectorFactory.createDenseZeros(treeSize);
    for (int treeNode = 0; treeNode < treeSize; treeNode++) {
      bestFeatures.set(treeNode, gbTree.get(treeNode).getLeft());
    }
    pushBestFeatureList.add(bestFeatures);
    parameterWorker.push(2 * label + 1, pushBestFeatureList);
  }

  /**
   * Push the bestSplitValue list to the server.
   * bestSplitValue will be placed in the index-2 of parameter server.
   */
  private void pushBestSplitValues(final int label) {
    final List<Vector> pushBestSplitValueList = new LinkedList<>();
    pushBestSplitValueList.add(vectorFactory.createDenseOnes(1));
    final Vector bestSplitValues = vectorFactory.createDenseZeros(treeSize);
    for (int treeNode = 0; treeNode < treeSize; treeNode++) {
      bestSplitValues.set(treeNode, gbTree.get(treeNode).getRight());
    }
    pushBestSplitValueList.add(bestSplitValues);
    parameterWorker.push(2 * label + 2, pushBestSplitValueList);
  }

  /**
   * Pull all the trees in the server and store them in the forest.
   */
  private void pullAllTrees(final int label) {
    forest.clear();
    final List<Vector> bestFeatureList = parameterWorker.pull(2 * label + 1);
    final List<Vector> bestSplitValueList = parameterWorker.pull(2 * label + 2);
    for (int i = 0; i < Math.min(bestFeatureList.size(), bestSplitValueList.size()); i++) {
      final GBTree semiTree = new GBTree(treeMaxDepth);
      for (int treeNode = 0; treeNode < treeSize; treeNode++) {
        semiTree.add(Pair.of((int)bestFeatureList.get(i).get(treeNode), bestSplitValueList.get(i).get(treeNode)));
      }
      forest.add(semiTree);
    }
  }

  /**
   * This method prints the expected y-value for each data based on the trees that are built.
   */
  private void testTrainingCode(final List<GBTData> instances) {
    if (valueType == 0) {
      pullAllTrees(0);
      final double[] predictedValue = new double[numData];
      for (int i = 0; i < numData; i++) {
        predictedValue[i] = 0;
      }
      for (final GBTree thisTree : forest) {
        for (final GBTData instance : instances) {
          if (instance.getIdentity() != TYPE_LINE) {
            predictedValue[instance.getIdentity()] += stepSize * predictByTree(instance, thisTree);
          }
        }
      }
      for (int i = 0; i < numData; i++) {
        LOG.log(Level.INFO, "Value {0} : {1}", new Object[]{i, predictedValue[i]});
      }
    } else {
      final double[][] predictedValue = new double[numData][valueType];
      for (int  i = 0; i < numData; i++) {
        for (int j = 0; j < valueType; j++) {
          predictedValue[i][j] = 0;
        }
      }
      for (int label = 0; label < valueType; label++) {
        pullAllTrees(label);
        for (final GBTree thisTree : forest) {
          for (final GBTData instance : instances) {
            if (instance.getIdentity() != TYPE_LINE) {
              predictedValue[instance.getIdentity()][label] += stepSize * predictByTree(instance, thisTree);
            }
          }
        }
      }
      for (int i = 0; i < numData; i++) {
        int predictedResult = 0;
        double maxValue = predictedValue[i][0];
        for (int j = 1; j < valueType; j++) {
          if (predictedValue[i][j] > maxValue) {
            maxValue = predictedValue[i][j];
            predictedResult = j;
          }
        }
        LOG.log(Level.INFO, "value {0} : {1}", new Object[]{i, predictedResult});
      }
    }
  }

  /**
   * Predict the y-value with the given instance and given tree.
   */
  private double predictByTree(final GBTData instance, final GBTree thisTree) {
    final Vector feature = instance.getFeature();
    int treeNode = 0;
    for (;;) {
      final Pair<Integer, Double> node = thisTree.get(treeNode);
      final int nodeFeature = node.getLeft();
      if (nodeFeature == NodeState.LEAF.getValue()) {
        return node.getRight();
      }
      if (featureType.get(nodeFeature) == 0) {
        final double splitValue = node.getRight();
        if (feature.get(nodeFeature) <= splitValue) {
          treeNode = 2 * treeNode + 1;
        } else {
          treeNode = 2 * treeNode + 2;
        }
      } else {
        final double getSplitValue = node.getRight();
        int splitValue = (int)getSplitValue;
        for (int i = 0; i < feature.get(nodeFeature); i++) {
          splitValue /= 2;
        }
        if (splitValue % 2 == 0) {
          treeNode = 2 * treeNode + 1;
        } else {
          treeNode = 2 * treeNode + 2;
        }
      }
    }
  }

  /**
   * Pull type of features(real number or class) and y-value.
   */
  private void pullTypeVectors() {
    featureType.clear();
    List<Vector> received = parameterWorker.pull(0);
    while (received.size() == 0) {
      received = parameterWorker.pull(0);
    }
    final Vector receivedType = received.get(0);
    for (int i = 0; i < numFeatures; i++) {
      featureType.add((int)receivedType.get(i));
    }
    valueType = (int)receivedType.get(numFeatures);
  }

  /**
   * Since there is only one updater, update feature type after fitting its shape adequately.
   */
  private void pushType() {
    final List<Vector> pushTypeList = new LinkedList<>();
    pushTypeList.add(vectorFactory.createDenseZeros(1));
    final Vector pushType = vectorFactory.createDenseZeros(numFeatures + 1);
    for (int i = 0; i < numFeatures; i++) {
      pushType.set(i, featureType.get(i));
    }
    pushType.set(numFeatures, valueType);
    pushTypeList.add(pushType);
    parameterWorker.push(0, pushTypeList);
  }

  /**
   * Make the given node into the leaf.
   */
  private void makeLeaf(final int treeNode) {
    double gSum = 0;
    final List<Integer> thisNode = dataTree.get(treeNode);
    for (final int leafMember : thisNode) {
      gSum += keyGPair.get(leafMember).getRight();
    }
    gbTree.add(Pair.of(NodeState.LEAF.getValue(), -gSum / (2 * thisNode.size() + lambda)));
    for (int i = 0; i < numFeatures; i++) {
      sortedByFeature.get(i).get(treeNode).clear();
    }
  }

  /**
   * Clean up all the data for the next run() iteration.
   */
  private void cleanAllDataForNextRun() {
    gbTree.clear();
    dataTree.clear();
    keyGPair.clear();
    for (final SortedTree tree : sortedByFeature) {
      tree.clear();
    }
    sortedByFeature.clear();
    labelList.clear();
    for (final GroupedTree tree : groupedByLabel) {
      tree.clear();
    }
    groupedByLabel.clear();
    for (final List<Integer> labels : labelList) {
      labels.clear();
    }
  }

  /**
   * Compute loss value using residual values.
   */
  private double computeLoss(final List<GBTData> instances) {
    double loss = 0;
    for (final GBTData instance : instances) {
      loss += residual.get(instance.getIdentity()) * residual.get(instance.getIdentity());
    }
    loss /= (2 * instances.size());
    return loss;
  }

  /**
   * Calculate the gain value with given data.
   */
  private double calculateGain(final double gL, final double gR, final int countLeft, final int countRight,
                               final double totalGain) {
    return (gL * gL / (2 * countLeft + lambda)) + (gR * gR / (2 * countRight + lambda)) - totalGain;
  }

  /**
   * This function creates binary value from position array(in the opposite order).
   * ex) if position array = {1, 0, 1, 1} then 13 (1101_(2)) will be made.
   */
  private int createBinary(final int[] position, final int numLabel) {
    int ret = 0;
    for (int  i = 0; i < numLabel; i++) {
      if (position[i] == 1) {
        ret += 1 << i;
      }
    }
    return ret;
  }

  /**
   * Return grey code with the length of codeSize.
   */
  private List<Integer> createGreyCode(final int codeSize) {
    final List<Integer> ret = new LinkedList<>();
    ret.add(0);
    ret.add(1);
    for (int i = 0; i < codeSize - 1; i++) {
      final List<Integer> temp = new LinkedList<>(ret);
      Collections.reverse(temp);
      for (int j = 0; j < temp.size(); j++) {
        temp.set(j, temp.get(j) + (1 << (i + 1)));
      }
      ret.addAll(temp);
    }
    return ret;
  }

  /**
   * Sort feature values in an ascending order.
   */
  private static final Comparator<Pair<Integer, Double>> FEATURE_COMPARATOR =
      (o1, o2) -> {
        if (o1.getRight() < o2.getRight()) {
          return -1;
        } else if (o1.getRight() > o2.getRight()) {
          return 1;
        } else {
          return 0;
        }
      };


  /**
   * This indicates each GBTree node's state.
   * If the node is empty, the node's state is EMPTY(-2).
   * If the node is leaf, the node's state is LEAF(-1).
   */
  private enum NodeState {
    EMPTY(-2), LEAF(-1);

    private final int value;

    NodeState(final int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }
}
