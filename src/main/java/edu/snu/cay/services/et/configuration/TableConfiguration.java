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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.configuration.parameters.*;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.HashBasedBlockPartitioner;
import edu.snu.cay.services.et.evaluator.impl.OrderingBasedBlockPartitioner;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.BuilderUtils;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A builder for configuration required for creating table.
 */
@Private
public final class TableConfiguration {
  private final String id;
  private final Class<? extends Codec> keyCodecClass;
  private final Class<? extends Codec> valueCodecClass;
  private final Class<? extends UpdateFunction> updateFunctionClass;
  private final boolean isOrderedTable;
  private final int numTotalBlocks;
  private final String filePath;
  private Configuration configuration = null;

  private TableConfiguration(final String id,
                             final Class<? extends Codec> keyCodecClass, final Class<? extends Codec> valueCodecClass,
                             final Class<? extends UpdateFunction> updateFunctionClass,
                             final boolean isOrderedTable,
                             final Integer numTotalBlocks, @Nullable final String filePath) {
    this.id = id;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
    this.updateFunctionClass = updateFunctionClass;
    this.isOrderedTable = isOrderedTable;
    this.numTotalBlocks = numTotalBlocks;
    this.filePath = filePath;
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
   * @return True if it's an ordered table, not a hashed table.
   */
  public boolean isOrderedTable() {
    return isOrderedTable;
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
    return Optional.ofNullable(filePath);
  }

  /**
   * @return a tang {@link Configuration} that includes all metadata of table
   */
  public Configuration getConfiguration() {
    if (configuration == null) {
      final Class<? extends BlockPartitioner> blockPartitionerClass = isOrderedTable ?
          OrderingBasedBlockPartitioner.class : HashBasedBlockPartitioner.class;

      configuration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(TableIdentifier.class, id)
          .bindNamedParameter(KeyCodec.class, keyCodecClass)
          .bindNamedParameter(ValueCodec.class, valueCodecClass)
          .bindImplementation(UpdateFunction.class, updateFunctionClass)
          .bindNamedParameter(IsOrderedTable.class, Boolean.toString(isOrderedTable))
          .bindImplementation(BlockPartitioner.class, blockPartitionerClass)
          .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
          .bindNamedParameter(FilePath.class, filePath != null ? filePath : FilePath.EMPTY)
          .build();
    }
    return configuration;
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
    private static final Logger LOG = Logger.getLogger(Builder.class.getName());

    /**
     * Required parameters.
     */
    private String id;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;
    private Class<? extends UpdateFunction> updateFunctionClass;
    private Boolean isOrderedTable;

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

    public Builder setIsOrderedTable(final Boolean isOrderedTable) {
      this.isOrderedTable = isOrderedTable;
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
      BuilderUtils.notNull(isOrderedTable);

      if (numTotalBlocks == null) {
        numTotalBlocks = Integer.valueOf(NumTotalBlocks.DEFAULT_VALUE_STR);
      }

      if (!isOrderedTable) {
        if (filePath != null) {
          LOG.log(Level.WARNING, "Initialization with bulk loading is not supported for hashed tables. " +
              "Will skip loading files in tableId: {0}", id);
          filePath = null;
        }
      }

      return new TableConfiguration(id, keyCodecClass, valueCodecClass,
          updateFunctionClass, isOrderedTable, numTotalBlocks, filePath);
    }
  }
}
