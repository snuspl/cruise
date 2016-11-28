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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.evaluator.api.PartitionFunction;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.util.BuilderUtils;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * A builder for configuration required for creating table.
 */
@Private
public final class TableConfiguration {
  private String id;
  private Class<? extends Codec> keyCodecClass;
  private Class<? extends Codec> valueCodecClass;
  private Class<? extends UpdateFunction> updateFunctionClass;
  private Class<? extends PartitionFunction> partitionFunctionClass;
  private int numTotalBlocks;
  private Optional<String> filePath;

  private TableConfiguration(final String id,
                             final Class<? extends Codec> keyCodecClass, final Class<? extends Codec> valueCodecClass,
                             final Class<? extends UpdateFunction> updateFunctionClass,
                             final Class<? extends PartitionFunction> partitionFunctionClass,
                             final Integer numTotalBlocks, @Nullable final String filePath) {
    this.id = id;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
    this.updateFunctionClass = updateFunctionClass;
    this.partitionFunctionClass = partitionFunctionClass;
    this.numTotalBlocks = numTotalBlocks;
    this.filePath = Optional.ofNullable(filePath);
  }

  /**
   * @return a table identifier
   */
  public String getId() {
    return id;
  }

  /**
   * @return a key codec
   */
  public Class<? extends Codec> getKeyCodecClass() {
    return keyCodecClass;
  }

  /**
   * @return a value codec
   */
  public Class<? extends Codec> getValueCodecClass() {
    return valueCodecClass;
  }

  /**
   * @return an update function
   */
  public Class<? extends UpdateFunction> getUpdateFunctionClass() {
    return updateFunctionClass;
  }

  /**
   * @return a partition function
   */
  public Class<? extends PartitionFunction> getPartitionFunctionClass() {
    return partitionFunctionClass;
  }

  /**
   * @return the number of blocks
   */
  public int getNumTotalBlocks() {
    return numTotalBlocks;
  }

  /**
   * @return a file path, which is optional
   */
  public Optional<String> getFilePath() {
    return filePath;
  }

  /**
   * @return a builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of TableConfiguration.
   */
  public static final class Builder implements org.apache.reef.util.Builder<TableConfiguration> {
    /**
     * Required parameters.
     */
    private String id;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;
    private Class<? extends UpdateFunction> updateFunctionClass;
    private Class<? extends PartitionFunction> partitionFunctionClass;

    /**
     * Optional parameters.
     */
    private Integer numTotalBlocks;
    private String filePath;

    private Builder() {
    }

    public Builder setId(final String id) {
      this.id = id;
      return this;
    }

    public Builder setKeyCodecClass(final Class<? extends Codec> keyCodecClass) {
      this.keyCodecClass = keyCodecClass;
      return this;
    }

    public Builder setValueCodecClass(final Class<? extends Codec> valueCodecClass) {
      this.valueCodecClass = valueCodecClass;
      return this;
    }

    public Builder setUpdateFunctionClass(final Class<? extends UpdateFunction> updateFunctionClass) {
      this.updateFunctionClass = updateFunctionClass;
      return this;
    }

    public Builder setPartitionFunctionClass(final Class<? extends PartitionFunction> partitionFunctionClass) {
      this.partitionFunctionClass = partitionFunctionClass;
      return this;
    }

    public Builder setNumTotalBlocks(final Integer numTotalBlocks) {
      this.numTotalBlocks = numTotalBlocks;
      return this;
    }

    public Builder setFilePath(final String filePath) {
      this.filePath = filePath;
      return this;
    }

    @Override
    public TableConfiguration build() {
      BuilderUtils.notNull(id);
      BuilderUtils.notNull(keyCodecClass);
      BuilderUtils.notNull(valueCodecClass);
      BuilderUtils.notNull(updateFunctionClass);
      BuilderUtils.notNull(partitionFunctionClass);

      if (numTotalBlocks == null) {
        numTotalBlocks = Integer.valueOf(NumTotalBlocks.DEFAULT_VALUE_STR);
      }

      return new TableConfiguration(id, keyCodecClass, valueCodecClass,
          updateFunctionClass, partitionFunctionClass, numTotalBlocks, filePath);
    }
  }
}
