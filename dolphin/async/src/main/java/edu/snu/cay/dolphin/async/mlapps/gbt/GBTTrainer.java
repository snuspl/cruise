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
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.mlapps.gbt.tree.*;
import edu.snu.cay.utils.Tuple3;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.mlapps.gbt.GBTParameters.*;

/**
 * {@link Trainer} class for the GBTREEF application.
 * Tree growing algorithm and boosting algorithm follows exact version of XGBoost.
 */
final class GBTTrainer implements Trainer<GBTData> {
  private static final Logger LOG = Logger.getLogger(GBTTrainer.class.getName());

  /**
   * Threshold value of label size. If a number of label types is smaller than this threshold, try all the possible
   * combinations of label types in bestSplitLabelFeature() function. Else, find the best split point greedily (passing
   * each feature one by one from the left child to the right child), yet less exact way.
   */
  private static final int LABEL_SIZE_THRESHOLD = 10;

  /**
   * CONTINUOUS_FEATURE : features with real-number type.
   */
  private static final int CONTINUOUS_FEATURE = 0;
  
  /**
   * Threshold for checking whether two double values are the same.
   */
  private static final double SIMILAR_VALUE_THRESHOLD = 1e-9;
  
  /**
   * Max iteration of run() method.
   */
  private final int maxNumEpochs;

  /**
   * The number of keys that are assigned to each tree for partitioning models across servers.
   */
  private final int numKeys;

  private final ModelAccessor<Integer, GBTree, List<GBTree>> modelAccessor;

  /**
   * Number of features.
   */
  private final int numFeatures;

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
   * If featureTypes.get(i) == CONTINUOUS, i-th feature type is real number.
   * If featureTypes.get(i) == CATEGORICAL, i-th feature type is categorical.
   */
  private final Map<Integer, FeatureType> featureTypes;

  /**
   * If valueType == 0, the type of y-value is real number (for regression).
   * If valueType != 0, the type of y-value is categorical (for classification).
   * The number of possible y values is equal to {@code valueType}
   * (e.g., if valueType == 5, y-value is in five categories)
   */
  private final FeatureType valueType;
  
  /**
   * If valueType == FeatureType.CONTINUOUS, valueTypeNum == 0
   * If valueType == FeatureType.CATEGORICAL, valueTypeNum is a number of value's label types.
   */
  private final int valueTypeNum;

  /**
   * Maximum depth of the tree(for regularization).
   */
  private final int treeMaxDepth;
  
  /**
   * Minimum size of leaf(for regularization).
   */
  private final int leafMinSize;

  /**
   * Tree size which is computed with treeMaxDepth.
   */
  private final int treeSize;

  private final Random random;

  @Inject
  private GBTTrainer(
                     final ModelAccessor<Integer, GBTree, List<GBTree>> modelAccessor,
                     @Parameter(NumFeatures.class) final int numFeatures,
                     @Parameter(StepSize.class) final double stepSize,
                     @Parameter(Lambda.class) final double lambda,
                     @Parameter(Gamma.class) final double gamma,
                     @Parameter(TreeMaxDepth.class) final int treeMaxDepth,
                     @Parameter(LeafMinSize.class) final int leafMinSize,
                     @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                     @Parameter(NumKeys.class) final int numKeys,
                     final GBTMetadataParser metadataParser) {
    this.modelAccessor = modelAccessor;
    this.numFeatures = numFeatures;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.gamma = gamma;
    this.treeMaxDepth = treeMaxDepth;
    this.leafMinSize = leafMinSize;
    this.maxNumEpochs = maxNumEpochs;
    this.numKeys = numKeys;
    this.treeSize = (1 << treeMaxDepth) - 1;
    this.random = new Random();
    final Pair<Map<Integer, FeatureType>, Integer> metaData = metadataParser.getFeatureTypes();
    this.featureTypes = metaData.getLeft();
    this.valueTypeNum = metaData.getRight();
    if (valueTypeNum == 0) {  // if valueTypeNum == 0, value's type is numerical(FeatureType.CONTINUOUS).
      this.valueType = FeatureType.CONTINUOUS;
    } else {  // if valueTypeNum != 0, value's type is categorical(FeatureType.CATEGORICAL).
      this.valueType = FeatureType.CATEGORICAL;
    }
  }

  @Override
  public void initGlobalSettings() {
  }

  @Override
  public void cleanup() {
  }

  /**
   * Build tree for this training data based on the trees that are already built before this run iteration.
   */
  @Override
  public MiniBatchResult runMiniBatch(final Collection<GBTData> miniBatchTrainingData) {
    final List<GBTData> instances = new ArrayList<>(miniBatchTrainingData);
    
    // Divide into two cases : Regression / Classification
    if (valueType == FeatureType.CONTINUOUS) {
      preprocessAndBuildTree(CONTINUOUS_FEATURE, instances);
    } else if (valueType == FeatureType.CATEGORICAL) {
      for (int label = 0; label < valueTypeNum; label++) {
        preprocessAndBuildTree(label, instances);
      }
    } else {
      throw new IllegalArgumentException("valueType must be either numerical type or categorical type.");
    }

    return MiniBatchResult.EMPTY_RESULT;
  }

  /**
   * Print the predicted value or label of each data in the epoch.
   *
   * @param epochTrainingData the training data that has been processed in the epoch
   * @param epochIdx the index of the epoch
   */
  @Override
  public EpochResult onEpochFinished(final Collection<GBTData> epochTrainingData,
                                     final Collection<GBTData> testData,
                                     final int epochIdx) {
    final List<GBTData> instances = new ArrayList<>(epochTrainingData);
    // This is for the test.
    if (epochIdx == (maxNumEpochs - 1)) {
      showPredictedValues(instances);
    }

    return EpochResult.EMPTY_RESULT;
  }

  /**
   * Following is the total procedure :
   * 1) Pull all the trees that are up-to-date and calculate residual values.
   * 2) Pre-process sorting and grouping.
   *    - if the feature type is real number, sort GBTData.
   *    - if the feature type is label, pre-calculate sum of g values for each class.
   * 3) Build tree using pre-processed data.
   * 4) Push the tree to the server.
   *
   * @param label If the (valueType == FeatureType.CONTINUOUS), {@param label} will have CONTINUOUS_FEATURE value.
   *              If the (valueType == FeatureType.CATEGORICAL), {@param label} will have a label value that this method
   *              is building a tree for.
   * @param instances All the input GBTData is included.
   */
  private void preprocessAndBuildTree(final int label, final List<GBTData> instances) {
    // Sort GBT data for each feature values and store it in sortedTreeList list.
    final List<SortedTree> sortedTreeList = new ArrayList<>();

    // For each feature, pre-count the number and pre-calculate g-values' sum of data that are belong to the each label.
    final List<GroupedTree> groupedTreeList = new ArrayList<>();

    // For each feature, if the feature is label type, store each data's label for that feature.
    final List<List<Integer>> labelList = new ArrayList<>();

    // Store g-values for each data.
    final List<Double> gValues = new ArrayList<>();

    final List<GBTree> forest = pullAllTrees(label);

    // Calculate residual values for each data in this worker using all the trees for this label.
    // residual = (real y-value) - (predicted y-value).
    // Predicted y-value is calculated by using all the trees that are built.
    final List<Double> residual = calculateResidual(instances, label, forest);

    // Get data from instances list and fill the following lists :
    // sortedTreeList, groupedTreeList, labelList, gValues, residual.
    initializePreprocessLists(sortedTreeList, groupedTreeList, labelList, gValues, residual, instances);

    // Pre-processing (prepare two tree lists to build tree):
    // For each feature type, pre-process the data for the following rules:
    //    - if the feature type is real number, sort training data depending on the feature values(sortedTreeList).
    //    - if the feature type is label type, group the data by its label and then calculate its g-values' sum and
    //      count the number of data in each label(groupedTreeList).
    preprocess(sortedTreeList, groupedTreeList, gValues, labelList);

    // This GBTree is a main tree that will be built in this method and pushed to the server at last.
    final GBTree gbTree = new GBTree(treeMaxDepth);

    // Build tree model using preprocessed data.
    final int miniBatchDataSize = instances.size();
    buildTree(gbTree, sortedTreeList, groupedTreeList, labelList, gValues, miniBatchDataSize);

    // Push a tree that is built in this run iteration to the server.
    pushTree(gbTree, label);
  }

  /**
   * Tree building procedure which minimizing object function.
   * For each tree node, find it's best split point and split the node.
   * If the node is inappropriate to split(by constraint such as max tree depth or min leaf size),
   * change the node to a leaf.
   */
  private void buildTree(final GBTree gbTree, final List<SortedTree> sortedTreeList,
                         final List<GroupedTree> groupedTreeList, final List<List<Integer>> labelList,
                         final List<Double> gValues, final int dataSize) {
    final DataTree dataTree = new DataTree(treeMaxDepth, dataSize);

    for (int nodeIdx = 0; nodeIdx < treeSize; nodeIdx++) {
      final int nodeSize = dataTree.get(nodeIdx).size();

      // if nodeSize == 0, node does not exist.
      if (nodeSize == 0) {
        gbTree.add(Pair.of(NodeState.EMPTY.getValue(), 0.0));
        continue;
      }

      // if nodeSize is smaller than leafMinSize or depth is deeper than treeMaxDepth, make the node to a leaf node.
      if (nodeSize <= leafMinSize || gbTree.getDepth(nodeIdx) >= treeMaxDepth) {
        gbTree.makeLeaf(nodeIdx, dataTree, gValues, lambda);
        clearLeafNode(sortedTreeList, groupedTreeList, nodeIdx);
        continue;
      }

      // Split this node by the best choice of a feature and a value.
      bestSplit(gbTree, dataTree, sortedTreeList, groupedTreeList, labelList, gValues, nodeIdx, dataSize);
    }
    
    dataTree.clear();
  }

  /**
   * Find the best split of the chosen node and split it.
   * You can find the best split by calculating Gain values for each feature and split value, and comparing each other.
   * If the maximum Gain value is negative, make the node to a leaf.
   *
   * @param nodeIdx : a node which we want to find the best split.
   */
  private void bestSplit(final GBTree gbTree, final DataTree dataTree, final List<SortedTree> sortedTreeList,
                         final List<GroupedTree> groupedTreeList, final List<List<Integer>> labelList,
                         final List<Double> gValues, final int nodeIdx, final int dataSize) {
    double gSum = 0;
    double bestGain = -gamma;
    int bestFeature = 0;
    double bestSplitValue = 0;
    final List<Integer> thisNode = dataTree.get(nodeIdx);
    final int nodeSize = thisNode.size();
    for (final int nodeMember : thisNode) {
      gSum += gValues.get(nodeMember);
    }
    final double totalGain = gSum * gSum / (2 * nodeSize + lambda) + gamma;

    // For each feature, find the best split point.
    // If the best split point in certain feature splits better than global best split point, then save the condition.
    for (int feature = 0; feature < numFeatures; feature++) {
      if (featureTypes.get(feature) == FeatureType.CONTINUOUS) {
        final List<Pair<Integer, Double>> sortedByFeature = sortedTreeList.get(feature).get(nodeIdx);
        final Tuple3<Double, Integer, Double> bestChoice = bestSplitRealNumberFeature(sortedByFeature, gValues,
            feature, gSum, totalGain, bestGain, bestFeature, bestSplitValue);
        bestGain = bestChoice.getFirst();
        bestFeature = bestChoice.getSecond();
        bestSplitValue = bestChoice.getThird();
      } else {
        final List<Pair<Integer, Double>> groupedByLabel = groupedTreeList.get(feature).get(nodeIdx);
        final Tuple3<Double, Integer, Double> bestChoice = bestSplitLabelFeature(groupedByLabel, nodeSize, feature,
            gSum, totalGain, bestGain, bestFeature, bestSplitValue);
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
    final ChildPosition[] position = new ChildPosition[dataSize];
    if (bestGain > 0) {
      gbTree.add(Pair.of(bestFeature, bestSplitValue));
      splitActualDataAndSetPosition(dataTree, sortedTreeList, groupedTreeList, labelList, nodeIdx, bestFeature,
                                    bestSplitValue, position);
      splitPreprocessedData(dataTree, sortedTreeList, groupedTreeList, labelList, gValues, nodeIdx, position);
    } else {
      gbTree.makeLeaf(nodeIdx, dataTree, gValues, lambda);
      clearLeafNode(sortedTreeList, groupedTreeList, nodeIdx);
    }

    // For the memory efficiency, clear thisNode since the node list will not be used anymore.
    thisNode.clear();
  }

  /**
   * Split the nodeIdx with the given feature(feature type is real number).
   * Initial condition : Empty left child / All the members in dataTree(nodeIdx) are in right child.
   * As passing each data in the right child one by one to the left child, calculate the tempGain.
   * If the tempGain is larger than the global gain(in here, retGain), change the condition for the best condition.
   * Left child and right child must have at least one member(they must not be empty).
   */
  private Tuple3<Double, Integer, Double> bestSplitRealNumberFeature(final List<Pair<Integer, Double>> sortedByFeature,
                                                                     final List<Double> gValues, final int feature,
                                                                     final double gSum, final double totalGain,
                                                                     final double bestGain, final int bestFeature,
                                                                     final double bestSplitValue) {
    final int nodeSize = sortedByFeature.size();
    double retGain = bestGain;
    int retFeature = bestFeature;
    double retSplitValue = bestSplitValue;
    double gL = 0;
    
    for (int dataIdx = 0; dataIdx < nodeSize - 1; dataIdx++) {
      boolean childNotExistError = false;
      final Pair<Integer, Double> data = sortedByFeature.get(dataIdx);
      gL += gValues.get(data.getLeft());
      while (similarValues(sortedByFeature.get(dataIdx + 1).getRight(), data.getRight())) {
        gL += gValues.get(sortedByFeature.get(++dataIdx).getLeft());
        if (dataIdx >= nodeSize - 1) {
          childNotExistError = true; break;
        }
      }
      if (!childNotExistError) {
        final double tempGain = calculateGain(gL, gSum - gL, dataIdx + 1, nodeSize - (dataIdx + 1), totalGain);
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
   * Split the nodeIdx with the given feature(feature type is label).
   *
   * Initial Condition : All the members in dataTree(nodeIdx) are in left child / Empty right child.
   * For every combinations of the feature's label set, calculate the tempGain.
   * If the tempGain is larger than the global gain(in here, retGain), change the condition for the best condition.
   * To travel all the possible combinations, gray code is used.
   * Since only one feature changing occurs as iteration goes along, tempGain can be calculated efficiently.
   * Left child and right child must have at least one member(they must not be empty).
   *
   * If the number of label type is smaller than LABEL_SIZE_THRESHOLD, try all the combinations of label types.
   * Else, find the best type to pass for each iteration from left child to right child as passing each type one by one.
   */
  private Tuple3<Double, Integer, Double> bestSplitLabelFeature(final List<Pair<Integer, Double>> groupedByLabel,
                                                                final int nodeSize, final int feature,
                                                                final double gSum, final double totalGain,
                                                                final double bestGain, final int bestFeature,
                                                                final double bestSplitValue) {
    double retGain = bestGain;
    int retFeature = bestFeature;
    double retSplitValue = bestSplitValue;
    double gL = gSum;
    int countLeft = nodeSize;
    final int numLabel = groupedByLabel.size();
    if (groupedByLabel.size() <= LABEL_SIZE_THRESHOLD) {
      final List<Integer> grayCode = createGrayCode(numLabel);
      for (int i = 1; i < grayCode.size(); i++) {
        int changedFeature = 0;
        int changedDigit = grayCode.get(i) ^ grayCode.get(i - 1);
        while (true) {
          if (changedDigit % 2 == 1) {
            break;
          }
          changedDigit /= 2;
          changedFeature++;
        }
        final int addOrSubtract;
        if (grayCode.get(i) > grayCode.get(i - 1)) {
          addOrSubtract = 1;
        } else {
          addOrSubtract = -1;
        }
        final Pair<Integer, Double> changedLabel = groupedByLabel.get(changedFeature);
        countLeft -= addOrSubtract * changedLabel.getLeft();
        gL -= addOrSubtract * changedLabel.getRight();
        if (countLeft == 0 || countLeft == nodeSize) {
          continue;
        }
        final double tempGain = calculateGain(gL, gSum - gL, countLeft, nodeSize - countLeft, totalGain);
        if (tempGain > retGain) {
          retGain = tempGain;
          retFeature = feature;
          retSplitValue = grayCode.get(i).doubleValue();
        }
      }
    } else {
      double preGain = 0;
      final ChildPosition[] position = new ChildPosition[numLabel];
      for (int i = 0; i < numLabel; i++) {
        position[i] = ChildPosition.LEFT_CHILD;
      }
      for (int i = 0; i < numLabel - 1; i++) {
        double bestDiff = -1;
        int changedLabel = -1;
        for (int label = 0; label < numLabel; label++) {
          if (position[label] == ChildPosition.LEFT_CHILD) {
            final int changedNum = groupedByLabel.get(label).getLeft();
            final double changedG = groupedByLabel.get(label).getRight();
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
          position[changedLabel] = ChildPosition.RIGHT_CHILD;
          final int changedNum = groupedByLabel.get(changedLabel).getLeft();
          final double changedG = groupedByLabel.get(changedLabel).getRight();
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
   * Split the actual data in dataTree and set each data's position whether to go LEFT_CHILD or RIGHT_CHILD.
   */
  private void splitActualDataAndSetPosition(final DataTree dataTree, final List<SortedTree> sortedTreeList,
                                             final List<GroupedTree> groupedTreeList,
                                             final List<List<Integer>> labelList, final int nodeIdx,
                                             final int bestFeature, final double bestSplitValue,
                                             final ChildPosition[] position) {
    final List<Integer> thisNode = dataTree.get(nodeIdx);
    final List<Integer> leftChild = dataTree.leftChild(nodeIdx);
    final List<Integer> rightChild = dataTree.rightChild(nodeIdx);

    if (featureTypes.get(bestFeature) == FeatureType.CONTINUOUS) {
      for (final Pair<Integer, Double> data : sortedTreeList.get(bestFeature).get(nodeIdx)) {
        if (data.getRight() <= bestSplitValue) {
          leftChild.add(data.getLeft());
          position[data.getLeft()] = ChildPosition.LEFT_CHILD;
        } else {
          rightChild.add(data.getLeft());
          position[data.getLeft()] = ChildPosition.RIGHT_CHILD;
        }
      }
    } else {
      final ChildPosition[] classPosition = new ChildPosition[groupedTreeList.get(bestFeature).get(nodeIdx).size()];
      for (int i = 0; i < classPosition.length; i++) {
        classPosition[i] = ChildPosition.LEFT_CHILD;
      }
      int tempBestSplitValue = (int)bestSplitValue;
      for (int i = 0; i < classPosition.length; i++) {
        if (tempBestSplitValue == 0) {
          break;
        }
        if (tempBestSplitValue % 2 == 1) {
          classPosition[i] = ChildPosition.RIGHT_CHILD;
        }
        tempBestSplitValue /= 2;
      }
      for (final int dataName : thisNode) {
        if (classPosition[labelList.get(bestFeature).get(dataName)] == ChildPosition.LEFT_CHILD) {
          leftChild.add(dataName);
          position[dataName] = ChildPosition.LEFT_CHILD;
        } else {
          rightChild.add(dataName);
          position[dataName] = ChildPosition.RIGHT_CHILD;
        }
      }
    }
  }

  /**
   * Split the pre-processed data when the node splits into two children.
   */
  private void splitPreprocessedData(final DataTree dataTree, final List<SortedTree> sortedTreeList,
                                     final List<GroupedTree> groupedTreeList, final List<List<Integer>> labelList,
                                     final List<Double> gValues, final int nodeIdx, final ChildPosition[] position) {
    final List<Integer> thisNode = dataTree.get(nodeIdx);
    for (int i = 0; i < numFeatures; i++) {
      if (featureTypes.get(i) == FeatureType.CONTINUOUS) {
        final List<Pair<Integer, Double>> thisSortedFeature = sortedTreeList.get(i).get(nodeIdx);
        final List<Pair<Integer, Double>> leftChildSorted = sortedTreeList.get(i).leftChild(nodeIdx);
        final List<Pair<Integer, Double>> rightChildSorted = sortedTreeList.get(i).rightChild(nodeIdx);
        for (final Pair<Integer, Double> data : thisSortedFeature) {
          if (position[data.getLeft()] == ChildPosition.LEFT_CHILD) {
            leftChildSorted.add(data);
          } else {
            rightChildSorted.add(data);
          }
        }
        thisSortedFeature.clear();
      } else {
        final List<Pair<Integer, Double>> thisGroupedLabel = groupedTreeList.get(i).get(nodeIdx);
        final List<Pair<Integer, Double>> leftChild = groupedTreeList.get(i).leftChild(nodeIdx);
        final List<Pair<Integer, Double>> rightChild = groupedTreeList.get(i).rightChild(nodeIdx);
        for (final int data : thisNode) {
          final int label = labelList.get(i).get(data);
          final double gValue = gValues.get(data);
          if (position[data] == ChildPosition.LEFT_CHILD) {
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
   * Set the pre-process lists using instances list.
   */
  private void initializePreprocessLists(final List<SortedTree> sortedTreeList,
                                         final List<GroupedTree> groupedTreeList, final List<List<Integer>> labelList,
                                         final List<Double> gValues, final List<Double> residual,
                                         final List<GBTData> instances) {
    for (int i = 0; i < numFeatures; i++) {
      sortedTreeList.add(new SortedTree(treeMaxDepth));
      groupedTreeList.add(new GroupedTree(treeMaxDepth));
      labelList.add(new ArrayList<>());
    }
    
    int dataIdx = 0;
    for (final GBTData instance : instances) {
      final Vector featureVector = instance.getFeature();
      gValues.add(-2.0 * residual.get(dataIdx));
      for (int feature = 0; feature < numFeatures; feature++) {
        if (featureTypes.get(feature) == FeatureType.CONTINUOUS) {
          sortedTreeList.get(feature).root().add(Pair.of(dataIdx, featureVector.get(feature)));
        } else {
          labelList.get(feature).add((int)featureVector.get(feature));
        }
      }
      dataIdx++;
    }
  }

  /**
   * For the root node of the trees, sort data by feature values or group data with their label values and calculate
   * some necessary values(sum of g values and number of data that are involved in the according label).
   */
  private void preprocess(final List<SortedTree> sortedTreeList, final List<GroupedTree> groupedTreeList,
                          final List<Double> gValues, final List<List<Integer>> labelList) {
    for (int feature = 0; feature < numFeatures; feature++) {
      if (featureTypes.get(feature) == FeatureType.CONTINUOUS) {
        sortedTreeList.get(feature).root().sort(FEATURE_COMPARATOR);
      } else {
        final List<Pair<Integer, Double>> groupedTreeRoot = groupedTreeList.get(feature).root();
        int instance = 0;
        for (final int label : labelList.get(feature)) {
          final int existingLabelNum = groupedTreeRoot.size();
          if (label >= existingLabelNum) {
            final int widenLength = label + 1 - existingLabelNum;
            for (int i = 0; i < widenLength; i++) {
              groupedTreeRoot.add(Pair.of(0, 0.0));
            }
          }
          final int oldNum = groupedTreeRoot.get(label).getLeft();
          final double oldGValue = groupedTreeRoot.get(label).getRight();
          final double addedGValue = gValues.get(instance++);
          groupedTreeRoot.set(label, Pair.of(oldNum + 1, oldGValue + addedGValue));
        }
      }
    }
    // For other nodes of the tree, set the node with a list filled with 0 values(length is the same with the root
    // node's list).
    // This is for more efficient and convenient process in buildTree() method.
    for (int feature = 0; feature < numFeatures; feature++) {
      if (featureTypes.get(feature) == FeatureType.CATEGORICAL) {
        final int existingLabelNum = groupedTreeList.get(feature).root().size();
        for (int node = 1; node < treeSize; node++) {
          for (int i = 0; i < existingLabelNum; i++) {
            groupedTreeList.get(feature).get(node).add(Pair.of(0, 0.0));
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
  private List<Double> calculateResidual(final List<GBTData> instances, final int label, final List<GBTree> forest) {
    final List<Double> residual = new ArrayList<>(instances.size());
    final List<Double> predictedValue = new ArrayList<>();
    boolean isFirstTree = true;
    for (final GBTree thisTree : forest) {
      int dataIdx = 0;
      for (final GBTData instance : instances) {
        if (isFirstTree) {
          predictedValue.add(0.0);
        }
        final double originalValue = predictedValue.get(dataIdx);
        predictedValue.set(dataIdx++,  originalValue + stepSize * predictByTree(instance, thisTree));
      }
      isFirstTree = false;
    }

    int dataIdx = 0;
    for (final GBTData instance : instances) {
      final double thisValue = instance.getValue();
      if (forest.isEmpty()) {
        residual.add(thisValue);
        continue;
      }
      final double thisPredictedValue = predictedValue.get(dataIdx++);
      if (valueType == FeatureType.CONTINUOUS) {
        residual.add(thisValue - thisPredictedValue);
      } else {
        if (similarValues(thisValue, label)) {
          residual.add(1 - thisPredictedValue);
        } else {
          residual.add(-thisPredictedValue);
        }
      }
    }
    return residual;
  }

  /**
   * Push the tree that is built in this run() iteration.
   * Randomly pick one key to store a GBTree and push the GBTree to the chosen key.
   */
  private void pushTree(final GBTree gbTree, final int label) {
    final int chosenKey = random.nextInt(numKeys);
    modelAccessor.push(label * numKeys + chosenKey, gbTree);
  }

  /**
   * Pull all the trees in the server and store them in the forest.
   */
  private List<GBTree> pullAllTrees(final int label) {
    final List<GBTree> forest = new LinkedList<>();
    for (int i = 0; i < numKeys; i++) {
      forest.addAll(modelAccessor.pull(label * numKeys + i));
    }
    return forest;
  }

  /**
   * This method prints the expected y-value for each data based on the trees that are built.
   */
  private void showPredictedValues(final List<GBTData> instances) {
    final int epochDataSize = instances.size();
    if (valueType == FeatureType.CONTINUOUS) {
      final List<GBTree> forest = pullAllTrees(0);
      final double[] predictedValue = new double[epochDataSize];
      for (int i = 0; i < epochDataSize; i++) {
        predictedValue[i] = 0;
      }
      for (final GBTree thisTree : forest) {
        int dataIdx = 0;
        for (final GBTData instance : instances) {
          predictedValue[dataIdx++] += stepSize * predictByTree(instance, thisTree);
        }
      }
      int dataIdx = 0;
      for (final GBTData instance : instances) {
        LOG.log(Level.INFO, "Predicted value : {0}", new Object[]{predictedValue[dataIdx++]});
        LOG.log(Level.INFO, "real value : {0}", instance.getValue());
      }
    } else {
      int misclassifiedNum = 0;
      final double[][] predictedValue = new double[epochDataSize][valueTypeNum];
      for (int  i = 0; i < epochDataSize; i++) {
        for (int j = 0; j < valueTypeNum; j++) {
          predictedValue[i][j] = 0;
        }
      }
      for (int label = 0; label < valueTypeNum; label++) {
        final List<GBTree> forest = pullAllTrees(label);
        for (final GBTree thisTree : forest) {
          int dataIdx = 0;
          for (final GBTData instance : instances) {
            predictedValue[dataIdx++][label] += stepSize * predictByTree(instance, thisTree);
          }
        }
      }
      int dataIdx = 0;
      for (final GBTData instance : instances) {
        int predictedResult = 0;
        double maxValue = predictedValue[dataIdx][0];
        for (int label = 1; label < valueTypeNum; label++) {
          if (predictedValue[dataIdx][label] > maxValue) {
            maxValue = predictedValue[dataIdx][label];
            predictedResult = label;
          }
        }
        if (!similarValues(predictedResult, instance.getValue())) {
          misclassifiedNum++;
        }
        LOG.log(Level.INFO, "Predicted class : {0}", new Object[]{predictedResult});
        LOG.log(Level.INFO, "real class : {0}", (int)instance.getValue());
        dataIdx++;
      }
      LOG.log(Level.INFO, "number of misclassified data : {0}, error rate : {1}",
              new Object[]{misclassifiedNum, (double) misclassifiedNum / epochDataSize});
    }
  }

  /**
   * Predict the y-value with the given instance and given tree.
   */
  private double predictByTree(final GBTData instance, final GBTree thisTree) {
    final Vector feature = instance.getFeature();
    int nodeIdx = 0;
    while (true) {
      final Pair<Integer, Double> node = thisTree.get(nodeIdx);
      final int nodeFeature = node.getLeft();
      if (nodeFeature == NodeState.LEAF.getValue()) {
        return node.getRight();
      }
      if (featureTypes.get(nodeFeature) == FeatureType.CONTINUOUS) {
        final double splitValue = node.getRight();
        if (feature.get(nodeFeature) <= splitValue) {
          nodeIdx = 2 * nodeIdx + 1;
        } else {
          nodeIdx = 2 * nodeIdx + 2;
        }
      } else {
        final double getSplitValue = node.getRight();
        int splitValue = (int)getSplitValue;
        for (int i = 0; i < feature.get(nodeFeature); i++) {
          splitValue /= 2;
        }
        if (splitValue % 2 == 0) {  // if the splitValue is an even number, the instance goes to the left child.
          nodeIdx = 2 * nodeIdx + 1;
        } else {
          nodeIdx = 2 * nodeIdx + 2;
        }
      }
    }
  }

  /**
   * Clear all the data at nodeIdx node of sortedTreeList and groupedTreeList(for memory efficiency).
   */
  private void clearLeafNode(final List<SortedTree> sortedTreeList, final List<GroupedTree> groupedTreeList,
                             final int nodeIdx) {
    for (int i = 0; i < numFeatures; i++) {
      sortedTreeList.get(i).get(nodeIdx).clear();
      groupedTreeList.get(i).get(nodeIdx).clear();
    }
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
  private int createBinary(final ChildPosition[] position, final int numLabel) {
    int ret = 0;
    for (int  i = 0; i < numLabel; i++) {
      if (position[i] == ChildPosition.RIGHT_CHILD) {
        ret += 1 << i;
      }
    }
    return ret;
  }
  
  /**
   * @param codeSize codeSize-bit gray code is produced in this method.
   * @return List of gray code sequence. The size of the list is 2^{@param codeSize}.
   */
  private List<Integer> createGrayCode(final int codeSize) {
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
   * @return If d1 == d2, return true. Else, return false.
   */
  private boolean similarValues(final double d1, final double d2) {
    return Math.abs(d1 - d2) < SIMILAR_VALUE_THRESHOLD;
  }

  public enum FeatureType {
    CONTINUOUS, CATEGORICAL
  }

  public enum ChildPosition {
    LEFT_CHILD, RIGHT_CHILD
  }
}
