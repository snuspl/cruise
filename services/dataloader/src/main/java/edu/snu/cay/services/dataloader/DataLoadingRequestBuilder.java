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
package edu.snu.cay.services.dataloader;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.data.loading.api.DistributedDataSet;
import org.apache.reef.io.data.loading.api.EvaluatorToPartitionStrategy;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartitionSerializer;
import org.apache.reef.io.data.loading.impl.AvroEvaluatorRequestSerializer;
import org.apache.reef.io.data.loading.impl.SingleDataCenterEvaluatorToPartitionStrategy;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartition;
import org.apache.reef.io.data.loading.impl.InputFormatLoadingService;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.data.loading.impl.MultiDataCenterEvaluatorToPartitionStrategy;
import org.apache.reef.runtime.common.utils.Constants;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Builder to create a request to the DataLoadingService.
 */
public final class DataLoadingRequestBuilder
    implements org.apache.reef.util.Builder<Configuration> {

  // constant used in several places.
  private static final int UNINITIALIZED = -1;
  private int numberOfDesiredSplits = UNINITIALIZED;
  private final List<EvaluatorRequest> computeRequests = new ArrayList<>();
  private final List<EvaluatorRequest> dataRequests = new ArrayList<>();
  private boolean inMemory = false;
  private boolean renewFailedEvaluators = true;
  private ConfigurationModule driverConfigurationModule = null;
  private String inputFormatClass;
  /**
   * Single data center loading strategy flag. Allows to specify if the data
   * will be loaded in machines of a single data center or not. By
   * default, is set to true.
   */
  private boolean singleDataCenterStrategy = true;
  /**
   * Distributed dataset that can contain many distributed partitions.
   */
  private DistributedDataSet distributedDataSet;

  /**
   * The input path of the data to be loaded.
   */
  private String inputPath;

  public DataLoadingRequestBuilder setNumberOfDesiredSplits(final int numberOfDesiredSplits) {
    this.numberOfDesiredSplits = numberOfDesiredSplits;
    return this;
  }

  /**
   * Adds the requests to the compute requests list.
   *
   * @param computeRequests
   *          the compute requests to add
   * @return this
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder addComputeRequests(final List<EvaluatorRequest> computeRequests) {
    for (final EvaluatorRequest computeRequest : computeRequests) {
      addComputeRequest(computeRequest);
    }
    return this;
  }

  /**
   * Adds the requests to the data requests list.
   *
   * @param dataRequests
   *          the data requests to add
   * @return this
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder addDataRequests(final List<EvaluatorRequest> dataRequests) {
    for (final EvaluatorRequest dataRequest : dataRequests) {
      addDataRequest(dataRequest);
    }
    return this;
  }

  /**
   * Adds a single request to the compute requests list.
   *
   * @param computeRequest
   *          the compute request to add
   * @return this
   */
  public DataLoadingRequestBuilder addComputeRequest(final EvaluatorRequest computeRequest) {
    this.computeRequests.add(computeRequest);
    return this;
  }

  /**
   * Adds a single request to the data requests list.
   *
   * @param dataRequest
   *          the data request to add
   * @return this
   */
  public DataLoadingRequestBuilder addDataRequest(final EvaluatorRequest dataRequest) {
    this.dataRequests.add(dataRequest);
    return this;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder loadIntoMemory(final boolean inMemory) {
    this.inMemory = inMemory;
    return this;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder renewFailedEvaluators(final boolean renewFailedEvaluators) {
    this.renewFailedEvaluators = renewFailedEvaluators;
    return this;
  }

  public DataLoadingRequestBuilder setDriverConfigurationModule(
      final ConfigurationModule driverConfigurationModule) {
    this.driverConfigurationModule = driverConfigurationModule;
    return this;
  }

  public DataLoadingRequestBuilder setInputFormatClass(
      final Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass.getName();
    return this;
  }

  /**
   * Sets the path of the folder where the data is.
   * Internally, a distributed dataset with a unique partition is created,
   * and {@link SingleDataCenterEvaluatorToPartitionStrategy} is binded.
   *
   * @param inputPath
   *          the input path
   * @return this
   */
  public DataLoadingRequestBuilder setInputPath(final String inputPath) {
    this.inputPath = inputPath;
    this.singleDataCenterStrategy = true;
    return this;
  }

  /**
   * Sets the distributed data set.
   * Internally, a {@link MultiDataCenterEvaluatorToPartitionStrategy} is binded.
   *
   * @param distributedDataSet
   *          the distributed data set
   * @return this
   */
  public DataLoadingRequestBuilder setDistributedDataSet(final DistributedDataSet distributedDataSet) {
    this.distributedDataSet = distributedDataSet;
    this.singleDataCenterStrategy = false;
    return this;
  }

  @Override
  public Configuration build() throws BindException {
    if (this.driverConfigurationModule == null) {
      throw new BindException("Driver Configuration Module is a required parameter.");
    }

    // need to create the distributed data set
    if (this.singleDataCenterStrategy) {
      if (this.inputPath == null) {
        throw new BindException("Should specify an input path.");
      }
      if (this.distributedDataSet != null && !this.distributedDataSet.isEmpty()) {
        throw new BindException("You should either call setInputPath or setDistributedDataSet, but not both");
      }
      // Create a distributed data set with one partition, the splits defined by
      // the user if greater than 0 or no splits, and data to be loaded from
      // anywhere.
      final DistributedDataSet dds = new DistributedDataSet();
      dds.addPartition(DistributedDataSetPartition
          .newBuilder()
          .setPath(inputPath)
          .setLocation(Constants.ANY_RACK)
          .setDesiredSplits(
              numberOfDesiredSplits > 0 ? numberOfDesiredSplits : Integer
                  .valueOf(NumberOfDesiredSplits.DEFAULT_DESIRED_SPLITS)).build());
      this.distributedDataSet = dds;
    } else {
      if (this.inputPath != null) {
        throw new BindException("You should either call setInputPath or setDistributedDataSet, but not both");
      }
    }

    if (this.distributedDataSet == null || this.distributedDataSet.isEmpty()) {
      throw new BindException("Distributed Data Set is a required parameter.");
    }

    if (this.inputFormatClass == null) {
      this.inputFormatClass = TextInputFormat.class.getName();
    }

    final Configuration driverConfiguration;
    if (renewFailedEvaluators) {
      driverConfiguration = this.driverConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DataLoader.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DataLoader.EvaluatorAllocatedHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, DataLoader.EvaluatorFailedHandler.class)
          .build();
    } else {
      driverConfiguration = this.driverConfigurationModule
          .set(DriverConfiguration.ON_DRIVER_STARTED, DataLoader.StartHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DataLoader.EvaluatorAllocatedHandler.class)
          .build();
    }

    final JavaConfigurationBuilder jcb =
        Tang.Factory.getTang().newConfigurationBuilder(driverConfiguration);

    Validate.isTrue(!this.dataRequests.isEmpty(),
        "Number of cores and memory are deprecated; you have to add specific data requests");
    for (final EvaluatorRequest request : this.dataRequests) {
      jcb.bindSetEntry(DataLoadingDataRequests.class, AvroEvaluatorRequestSerializer.toString(request));
    }

    // compute requests can be empty to maintain compatibility with previous code.
    if (!this.computeRequests.isEmpty()) {
      for (final EvaluatorRequest request : this.computeRequests) {
        jcb.bindSetEntry(DataLoadingComputeRequests.class, AvroEvaluatorRequestSerializer.toString(request));
      }
    }

    jcb.bindNamedParameter(LoadDataIntoMemory.class, Boolean.toString(this.inMemory))
        .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClass);

    final Iterator<DistributedDataSetPartition> partitions = this.distributedDataSet.iterator();
    while (partitions.hasNext()) {
      jcb.bindSetEntry(
          DistributedDataSetPartitionSerializer.DistributedDataSetPartitions.class,
          DistributedDataSetPartitionSerializer.serialize(partitions.next()));
    }

    // we do this check for backwards compatibility, if the user defined it
    // wants to use the single data center loading strategy, we bind that implementation.
    if (this.singleDataCenterStrategy) {
      jcb.bindImplementation(EvaluatorToPartitionStrategy.class, SingleDataCenterEvaluatorToPartitionStrategy.class);
    } else {
      // otherwise, we bind the strategy that will allow the user to specify
      // which evaluators can load the different partitions in a multi data center network topology
      jcb.bindImplementation(EvaluatorToPartitionStrategy.class, MultiDataCenterEvaluatorToPartitionStrategy.class);
    }

    return jcb.bindImplementation(DataLoadingService.class, InputFormatLoadingService.class).build();
  }

  @NamedParameter(short_name = "num_desired_splits", default_value = NumberOfDesiredSplits.DEFAULT_DESIRED_SPLITS)
  public static final class NumberOfDesiredSplits implements Name<Integer> {
    static final String DEFAULT_DESIRED_SPLITS = "0";
  }

  /**
   * Allows to specify a set of compute requests to send to the DataLoader.
   */
  @NamedParameter(doc = "Sets of compute requests to request to the DataLoader, " +
      "i.e. evaluators requests that will not load data")
  static final class DataLoadingComputeRequests implements Name<Set<String>> {
  }

  /**
   * Allows to specify a set of data requests to send to the DataLoader.
   */
  @NamedParameter(doc = "Sets of data requests to request to the DataLoader, " +
      "i.e. evaluators requests that will load data")
  static final class DataLoadingDataRequests implements Name<Set<String>> {
  }

  @NamedParameter(default_value = "false")
  public static final class LoadDataIntoMemory implements Name<Boolean> {
  }
}
