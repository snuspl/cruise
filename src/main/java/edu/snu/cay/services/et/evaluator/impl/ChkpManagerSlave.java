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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.chkp.ChkpCommitPath;
import edu.snu.cay.services.et.configuration.parameters.chkp.ChkpTempPath;
import edu.snu.cay.services.et.driver.impl.ChkpManagerMaster;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A executor-side checkpoint manager.
 * Following the order from {@link ChkpManagerMaster}, it executes checkpoint
 * of locally assigned blocks of a table and restoration from the checkpoint.
 *
 * When doing checkpoint, at first, it stores each block to a file in local filesystem.
 * Each checkpoint has its own directory and each block is saved into a file under the directory.
 * [{@link ChkpTempPath}/CheckpointID/BlockIdx]
 * Checkpoint is done when all associators finish this task.
 *
 * But this these checkpointed blocks can be lost, when this executor is closed by reconfiguration.
 * So executors commit them to a safe place (e.g., HDFS for Yarn runtime), when being closed.
 * [{@link ChkpCommitPath}/CheckpointID/BlockIdx]
 */
public final class ChkpManagerSlave {
  private static final Logger LOG = Logger.getLogger(ChkpManagerSlave.class.getName());
  private static final String CONF_FILE_NAME = "conf";

  private final InjectionFuture<Tables> tablesFuture;
  private final InjectionFuture<MessageSender> msgSenderFuture;
  private final ConfigurationSerializer confSerializer;
  private final String tempPath;
  private final String commitPath;
  private final boolean commitToHdfs;

  private final Set<Checkpoint> localCheckpoint = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Inject
  private ChkpManagerSlave(final InjectionFuture<Tables> tablesFuture,
                           final InjectionFuture<MessageSender> msgSenderFuture,
                           @Parameter(ChkpTempPath.class) final String chkpTempPath,
                           @Parameter(ChkpCommitPath.class) final String chkpCommitPath,
                           final ConfigurationSerializer confSerializer) {
    this.tablesFuture = tablesFuture;
    this.msgSenderFuture = msgSenderFuture;
    this.confSerializer = confSerializer;
    this.tempPath = chkpTempPath;
    this.commitPath = chkpCommitPath;
    this.commitToHdfs = chkpCommitPath.startsWith("hdfs://");
  }

  /**
   * A class that encapsulates the information of a locally saved portion of a checkpoint.
   */
  private final class Checkpoint {
    private final String checkpointId;
    private final List<Integer> blocks;

    Checkpoint(final String checkpointId, final List<Integer> blocks) {
      this.checkpointId = checkpointId;
      this.blocks = Collections.unmodifiableList(blocks);
    }

    String getCheckpointId() {
      return checkpointId;
    }

    List<Integer> getBlocks() {
      return blocks;
    }
  }

  private org.apache.reef.tang.Configuration readTableConf(final String chkpId) throws IOException {
    final Path baseDir = new Path(tempPath, chkpId);

    try (FileSystem fs = FileSystem.getLocal(new Configuration())) {
      try (FSDataInputStream fis = fs.open(new Path(baseDir, CONF_FILE_NAME))) {
        final int size = fis.readInt();
        final byte[] buffer = new byte[size];
        final int readBytes = fis.read(buffer);
        assert readBytes == size;
        return confSerializer.fromByteArray(buffer);
      }
    }
  }

  private void writeTableConf(final String chkpId,
                              final org.apache.reef.tang.Configuration tableConf) throws IOException {
    final Path baseDir = new Path(tempPath, chkpId);
    try (FileSystem fs = FileSystem.getLocal(new Configuration())) {
      fs.setWriteChecksum(false); // do not make crc file
      final byte[] serializedTableConf = confSerializer.toByteArray(tableConf);
      try (FSDataOutputStream fos = fs.create(new Path(baseDir, CONF_FILE_NAME))) {
        fos.writeInt(serializedTableConf.length);
        fos.write(serializedTableConf);
      }
    }
  }

  /**
   * Checkpoints a table with {@code tableId}.
   * More specifically, it checkpoints locally assigned blocks to a temporal file of local filesystem.
   * @param chkpId a checkpoint Id
   * @param tableId a table Id
   * @param <K> key type
   * @param <V> value type
   * @throws IOException when failed to write a checkpoint
   * @throws TableNotExistException when the table does not exist in this executor
   */
  <K, V> void checkpoint(final String chkpId, final String tableId) throws IOException, TableNotExistException {
    final Path baseDir = new Path(tempPath, chkpId);

    LOG.log(Level.INFO, "Start checkpointing. chkpId: {0}, tableId: {1}, baseDir: {2}",
        new Object[]{chkpId, tableId, baseDir});

    final TableComponents<K, V, ?> tableComponents = tablesFuture.get().getTableComponents(tableId);

    final List<Integer> chkpedBlocks = new LinkedList<>();
    try (FileSystem fs = FileSystem.getLocal(new Configuration())) {
      fs.setWriteChecksum(false); // do not make crc file

      writeTableConf(chkpId, tableComponents.getTableConf());

      final KVUSerializer<K, V, ?> kvuSerializer = tableComponents.getSerializer();
      final BlockStore<K, V, ?> blockStore = tableComponents.getBlockStore();

      final AtomicInteger numItems = new AtomicInteger(0);
      blockStore.iterator().forEachRemaining(block -> {
        // create a file for each block
        try (FSDataOutputStream fos = fs.create(new Path(baseDir, Integer.toString(block.getId())))) {
          fos.writeInt(block.getNumPairs());
          block.iterator().forEachRemaining(kvEntry -> {
            // TODO #215: use StreamingCodec
            ((StreamingCodec<K>) kvuSerializer.getKeyCodec()).encodeToStream(kvEntry.getKey(), fos);
            ((StreamingCodec<V>) kvuSerializer.getValueCodec()).encodeToStream(kvEntry.getValue(), fos);
          });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        numItems.getAndAdd(block.getNumPairs());
        chkpedBlocks.add(block.getId());
      });

      LOG.log(Level.INFO, "Checkpoint done. chkpId: {0}, tableId: {1}, numBlocks: {2}, numItems:{3}",
          new Object[]{chkpId, tableId, chkpedBlocks.size(), numItems.get()});
    }

    localCheckpoint.add(new Checkpoint(chkpId, chkpedBlocks));

    try {
      msgSenderFuture.get().sendChkpDoneMsg(chkpId, chkpedBlocks);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Commits all block checkpoints in local.
   * @throws IOException when fail to commit all checkpoints
   */
  void commitAllLocalChkps() throws IOException {
    LOG.log(Level.INFO, "Commit {0} checkpoints", localCheckpoint.size());
    for (final Checkpoint checkpoint : localCheckpoint) {
      commit(checkpoint);
    }
  }

  /**
   * Commit a checkpoint temporarily saved in a local filesystem.
   * @param chkp a checkpoint to commit
   * @throws IOException when fail to read checkpoint from temporal place and write it to commit place
   */
  private <K, V> void commit(final Checkpoint chkp) throws IOException {
    LOG.log(Level.INFO, "Start committing checkpoints. chkpId: {0}", chkp.getCheckpointId());

    final org.apache.reef.tang.Configuration tableConf = readTableConf(chkp.getCheckpointId());
    final Injector tableInjector = Tang.Factory.getTang().newInjector(tableConf);

    final KVUSerializer<K, V, ?> kvuSerializer;
    try {
      kvuSerializer = tableInjector.getInstance(KVUSerializer.class);
    } catch (InjectionException e) {
      throw new RuntimeException("Table conf file has been corrupted", e);
    }
    final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
    final Codec<V> valueCodec = kvuSerializer.getValueCodec();

    final Configuration hadoopConf = new Configuration();
    // read checkpoint files and put into a table
    try (FileSystem localFS = FileSystem.getLocal(hadoopConf);
         FileSystem fsToCommit = commitToHdfs ? FileSystem.get(hadoopConf) : localFS) {
      final Path baseDir = new Path(tempPath, chkp.getCheckpointId());
      for (final Integer blockId : chkp.getBlocks()) {

        final FSDataInputStream fis = localFS.open(new Path(baseDir, Integer.toString(blockId)));
        final int numItems = fis.readInt();
        final List<Pair<K, V>> kvList = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
          // TODO #215: use StreamingCodec
          final K key = ((StreamingCodec<K>) keyCodec).decodeFromStream(fis);
          final V value = ((StreamingCodec<V>) valueCodec).decodeFromStream(fis);
          kvList.add(Pair.of(key, value));
        }
        fis.close();

        writeToCommit(chkp.getCheckpointId(), blockId, kvList, kvuSerializer, fsToCommit);
      }
    }

    LOG.log(Level.INFO, "Checkpoint commit has done. chkpId: {0}, numCommittedBlocks: {1}",
        new Object[]{chkp.getCheckpointId(), chkp.getBlocks().size()});

    try {
      msgSenderFuture.get().sendChkpCommitMsg(chkp.getCheckpointId());
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  private <K, V> void writeToCommit(final String chkpId,
                                    final int blockId,
                                    final List<Pair<K, V>> kvList,
                                    final KVUSerializer<K, V, ?> kvuSerializer,
                                    final FileSystem fs) throws IOException {
    try (FSDataOutputStream fos = fs.create(new Path(commitPath, chkpId + "/" + Integer.toString(blockId)))) {
      fs.setWriteChecksum(false);
      fos.writeInt(kvList.size());
      kvList.iterator().forEachRemaining(kvEntry -> {
        // TODO #215: use StreamingCodec
        ((StreamingCodec<K>) kvuSerializer.getKeyCodec()).encodeToStream(kvEntry.getKey(), fos);
        ((StreamingCodec<V>) kvuSerializer.getValueCodec()).encodeToStream(kvEntry.getValue(), fos);
      });
    }
  }

  /**
   * Get a existing table, if this executor has it.
   * Otherwise it instantiates an one-time-use table.
   */
  private Pair<Table, TableComponents> getTable(final String tableId,
                                                final String chkpId,
                                                final List<String> ownershipStatus) throws IOException {
    TableComponents tableComponents;
    Table table;

    // It utilizes table access routine, when putting loaded k-v items into table.
    // Use existing table, if it exists
    try {
      table = tablesFuture.get().getTable(tableId);
      tableComponents = tablesFuture.get().getTableComponents(tableId);

      // if this executor does not have a table to be restored from the checkpoint
      // instantiate a table for one time use and throw it away
    } catch (TableNotExistException e) {
      final Pair<Table, TableComponents> tablePair;
      try {
        tablePair = tablesFuture.get().instantiateTable(readTableConf(chkpId), ownershipStatus);
        table = tablePair.getLeft();
        tableComponents = tablePair.getRight();
      } catch (InjectionException e1) {
        throw new RuntimeException("Table configuration is incomplete", e1);
      }
    }

    return Pair.of(table, tableComponents);
  }

  /**
   * Load a checkpoint that is in a temporal stage.
   */
  private void loadChkpInTemp(final String chkpId, final String tableId,
                              final List<String> ownershipStatus,
                              final List<Integer> blockIdsToLoad) throws IOException {

    final Path baseDir = new Path(tempPath, chkpId);

    LOG.log(Level.INFO, "Start chkp(temp) loading. chkpId: {0}, tableId: {1}, baseDir: {2}",
        new Object[]{chkpId, tableId, baseDir});

    final Pair<Table, TableComponents> tablePair = getTable(tableId, chkpId, ownershipStatus);
    final Table table = tablePair.getLeft();
    final TableComponents tableComponents = tablePair.getRight();

    final FileSystem fs = FileSystem.getLocal(new Configuration());
    final int numTotalItems = loadChkpIntoTable(table, tableComponents, blockIdsToLoad, baseDir, fs);

    LOG.log(Level.INFO, "Chkp(temp) load done. chkpId: {0}, tableId: {1}, numBlocks: {2}, numItems:{3}",
        new Object[]{chkpId, tableId, blockIdsToLoad.size(), numTotalItems});
  }

  /**
   * Load a checkpoint that is in a commit stage.
   */
  private void loadChkpInCommit(final String chkpId, final String tableId,
                                final List<Integer> blockIdsToLoad) throws IOException {

    final Path baseDir = new Path(commitPath, chkpId);

    LOG.log(Level.INFO, "Start chkp(commit) loading. chkpId: {0}, tableId: {1}, baseDir: {2}",
        new Object[]{chkpId, tableId, baseDir});

    final TableComponents tableComponents;
    final Table table;
    try {
      tableComponents = tablesFuture.get().getTableComponents(tableId);
      table = tablesFuture.get().getTable(tableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    final FileSystem fs = commitToHdfs ? FileSystem.get(new Configuration()) : FileSystem.getLocal(new Configuration());
    final int numTotalItems = loadChkpIntoTable(table, tableComponents, blockIdsToLoad, baseDir, fs);

    LOG.log(Level.INFO, "Chkp(commit) load done. chkpId: {0}, tableId: {1}, numBlocks: {2}, numItems:{3}",
        new Object[]{chkpId, tableId, blockIdsToLoad.size(), numTotalItems});
  }

  /**
   * Read k-v pairs from checkpoint and put them into a table.
   */
  private <K, V> int loadChkpIntoTable(final Table table, final TableComponents tableComponents,
                                       final List<Integer> blockIdsToLoad,
                                       final Path baseDir, final FileSystem fs) throws IOException {
    final List<Future> futureList = new LinkedList<>();
    final KVUSerializer<K, V, ?> kvuSerializer = tableComponents.getSerializer();
    final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
    final Codec<V> valueCodec = kvuSerializer.getValueCodec();

    int numTotalItems = 0;
    for (final Integer blockId : blockIdsToLoad) {
      try (FSDataInputStream fis = fs.open(new Path(baseDir, Integer.toString(blockId)))) {
        final int numItems = fis.readInt();
        final List<Pair<K, V>> kvList = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
          // TODO #215: use StreamingCodec
          final K key = ((StreamingCodec<K>) keyCodec).decodeFromStream(fis);
          final V value = ((StreamingCodec<V>) valueCodec).decodeFromStream(fis);
          kvList.add(Pair.of(key, value));
        }
        numTotalItems += numItems;
        futureList.add(table.multiPut(kvList));
      }
    }

    // wait until all multi-puts are finished
    for (final Future future : futureList) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }

    return numTotalItems;
  }

  /**
   * Load from a checkpoint created by {@link #checkpoint}.
   * It reads saved blocks and put them into table through table access routine (e.g., {@link Table#multiPut}).
   * Table configuration and block ownership status are required to do table access.
   * @param chkpId a checkpoint Id
   * @param tableId a table Id
   * @param ownershipStatus a block ownership status
   * @param blockIdsToLoad block ids to load from a checkpoint
   * @throws IOException when fail to read a checkpoint
   */
  void loadChkp(final String chkpId, final String tableId,
                              @Nullable final List<String> ownershipStatus,
                              final boolean committed,
                              final List<Integer> blockIdsToLoad) throws IOException {
    if (committed) {
      loadChkpInCommit(chkpId, tableId, blockIdsToLoad);
    } else {
      loadChkpInTemp(chkpId, tableId, ownershipStatus, blockIdsToLoad);
    }

    try {
      msgSenderFuture.get().sendChkpLoadDoneMsg(chkpId);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }
}
