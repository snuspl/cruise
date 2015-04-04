package org.apache.reef.inmemory.driver.metatree;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.FileMetaStatus;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.driver.BaseFsClient;
import org.apache.reef.inmemory.driver.CacheNode;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages all operations that has to do with Surf metadata(FileEntry, DirectoryEntry, FileMeta)
 */
public class MetaTree {
  private static final Logger LOG = Logger.getLogger(MetaTree.class.getName());

  private final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock(true); // TODO: replace this with a more fine-grained LOCK
  private final DirectoryEntry ROOT;
  private final HashMap<Long, FileMeta> fileIdToFileMeta = new HashMap<>();

  private final EventRecorder RECORD;// TODO: make use of this
  private final BaseFsClient baseFsClient;
  private final AtomicLong atomicFileId;

  @Inject
  public MetaTree(final BaseFsClient baseFsClient,
                  final EventRecorder recorder) {
    this.ROOT = new DirectoryEntry("/", null);
    this.RECORD = recorder;
    this.baseFsClient = baseFsClient;
    this.atomicFileId = new AtomicLong(0);
  }

  //////// Read-Lock Methods: Operations that only query the tree

  /**
   * Get the filemeta for the exact path (no directory allowed)
   *
   * @param path to the file
   * @return the FileMeta of the path in tree
   * @throws FileNotFoundException if no FileMeta for the exact path exists in tree
   */
  public FileMeta getFileMeta(final String path) throws FileNotFoundException {
    LOCK.readLock().lock();
    try {
      final Entry entry = getEntryInTree(path);
      if (entry != null && !entry.isDirectory()) {
        return ((FileEntry) entry).getFileMeta();
      } else {
        throw new FileNotFoundException("FileMeta does not exist in Surf MetaTree");
      }
    } finally {
      LOCK.readLock().unlock();
    }
  }

  /**
   * List entries at path
   *
   * @param path to a directory or a file
   * @return a list of FileMetaStatus
   * @throws FileNotFoundException if no such directory or file exists for the path
   */
  public List<FileMetaStatus> listFileMetaStatus(final String path) throws FileNotFoundException {
    LOCK.readLock().lock();
    try {
      final Entry entry = getEntryInTree(path);

      if (entry != null) {
        if (entry.isDirectory()) {
          final List<FileMetaStatus> fileMetaStatus = new ArrayList<>();

          if (((DirectoryEntry) entry).getChildren().size() > 0) {
            for (final Entry childEntry : ((DirectoryEntry) entry).getChildren()) {
              if (childEntry.isDirectory()) {
                fileMetaStatus.add(new FileMetaStatus(path + "/" + childEntry.getName(), null));
              } else {
                fileMetaStatus.add(new FileMetaStatus(path + "/" + childEntry.getName(), ((FileEntry) childEntry).getFileMeta()));
              }
            }
          } else {
            fileMetaStatus.add(new FileMetaStatus(path, null));
          }

          return fileMetaStatus;
        } else {
          final FileMeta fileMeta = ((FileEntry) entry).getFileMeta();
          return Arrays.asList(new FileMetaStatus(path, fileMeta));
        }
      } else {
        throw new FileNotFoundException("No directory or file found for the path " + path);
      }
    } finally {
      LOCK.readLock().unlock();
    }
  }

  public boolean exists(final String path) {
    LOCK.readLock().lock();
    try {
      return !(getEntryInTree(path) == null);
    } finally {
      LOCK.readLock().unlock();
    }
  }

  //////// Write-Lock Methods: Operations that update the tree

  /**
   * Get FileMeta from the tree or load it from Base if not exists
   */
  public FileMeta getOrLoadFileMeta(final String path) throws IOException {
    LOCK.readLock().lock();
    try {
      final Entry entry = getEntryInTree(path);
      if (entry != null && !entry.isDirectory()) {
        return ((FileEntry) entry).getFileMeta();
      }
    } finally {
      LOCK.readLock().unlock();
    }

    final FileMeta fileMeta = baseFsClient.getFileStatus(path); // throws FileNotFoundException/IOException, sets blockSize & fileSize
    final long fileId = atomicFileId.incrementAndGet();
    fileMeta.setFileId(fileId);

    LOCK.writeLock().lock();
    try {
      // Check the tree again as multiple threads could have executed baseFsClient.getFileStatus(path) for the same path
      final Entry entry = getEntryInTree(path);
      if (entry != null && !entry.isDirectory()) {
        return ((FileEntry) entry).getFileMeta();
      }

      registerNewFileMeta(path, fileMeta);
      return fileMeta;
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  /**
   * First, create a file in HDFS
   * Second, create a file in the tree
   */
  public void createFile(final String path, final long blockSize, final short baseFsReplication) throws IOException {
    // Exploit the lease in HDFS to prevent concurrent create(and thus write) of a file
    baseFsClient.create(path, baseFsReplication, blockSize); // TODO: hold onto the outputstream (close later)

    final FileMeta fileMeta = new FileMeta();
    final long fileId = atomicFileId.incrementAndGet();
    fileMeta.setFileId(fileId);
    fileMeta.setFileSize(0);
    fileMeta.setBlockSize(blockSize);
    fileMeta.setBlocks(new ArrayList<BlockMeta>());

    LOCK.writeLock().lock();
    try {
      registerNewFileMeta(path, fileMeta);
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  /**
   * First, create a directory in HDFS
   * Second, create a directory in the tree
   */
  public boolean mkdirs(final String path) throws IOException {
    LOCK.writeLock().lock();
    try {
      createDirectoryRecursively(path);
      return true;
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  /**
   * First, rename in HDFS
   * Second, rename in the tree
   */
  public boolean rename(final String src, final String dst) throws IOException {
    LOCK.writeLock().lock();
    try {
      final boolean baseSuccess = baseFsClient.rename(src, dst);

      if (baseSuccess) {
        final Entry srcEntry = getEntryInTree(src);
        final boolean srcExists = !(srcEntry == null);
        final Entry dstEntry = getEntryInTree(dst);
        final boolean dstExists = !(dstEntry == null);

        if (srcExists) {
          if (dstExists) {
            if (srcEntry.isDirectory() == dstEntry.isDirectory()) {
              final DirectoryEntry dstParent = dstEntry.getParent();
              dstParent.removeChild(dstEntry);
              srcEntry.rename(dstEntry.getName(), dstEntry.getParent());
              return true;
            } else {
              return false; // src and dst should be either both file or directory
            }
          } else {
            final int index = dst.lastIndexOf('/');
            final String dstParentPath = dst.substring(0, index);
            final String dstFileName = dst.substring(index+1, dst.length());
            final DirectoryEntry dstParent = getOrCreateDirectoryRecursively(dstParentPath);
            srcEntry.rename(dstFileName, dstParent);
            return true;
          }
        } else {
          return false;
        }
      } else {
        return false;
      }
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  /**
   * First, delete in HDFS
   * Second, delete in the tree
   */
  public boolean remove(final String path, final boolean recursive) throws IOException {
    final boolean baseFsSuccess = baseFsClient.delete(path);
    LOCK.writeLock().lock();
    try {
      if (baseFsSuccess) {
        final Entry entry = getEntryInTree(path);
        if (entry != null) {
          if (entry.isDirectory()) {
            if (recursive) {
              entry.getParent().removeChild(entry);
              return true;
            } else {
              throw new IOException("Recursive not set to true while the path is to a directory");
            }
          } else {
            entry.getParent().removeChild(entry);
            fileIdToFileMeta.remove(((FileEntry) entry).getFileMeta().getFileId());
            return true;
          }
        } else {
          LOG.log(Level.INFO, "Attempted to delete FileEntry that does not exist in the tree", path);
          return true;
        }
      } else {
        return false;
      }
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  public int unCacheAll() {
    LOCK.writeLock().lock();
    try {
      final int numOfEntries = this.fileIdToFileMeta.size();
      this.fileIdToFileMeta.clear();
      this.ROOT.removeAllChildren();
      return numOfEntries;
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  /**
   * Add newly written blocks reported by CacheNodes
   */
  public void addNewWrittenBlockToFileMetaInTree(final BlockId blockId, final long nWritten, final CacheNode cacheNode) {
    final FileMeta fileMeta = fileIdToFileMeta.get(blockId.getFileId());
    if (fileMeta != null) {
      final List<NodeInfo> nodeList = Arrays.asList(new NodeInfo(cacheNode.getAddress(), cacheNode.getRack()));
      final BlockMeta blockMeta = new BlockMeta(blockId.getFileId(), blockId.getOffset(), fileMeta.getBlockSize(), nodeList); // TODO: check replication when we implement replicated write
      synchronized (fileMeta) {
        fileMeta.setFileSize(fileMeta.getFileSize() + nWritten);
        fileMeta.addToBlocks(blockMeta);
      }
    } else {
      // TODO: we may want to return boolean here for upstream handling
    }
  }

  //////// Helper Methods

  /**
   * The caller of this method must hold LOCK.
   *
   * @param path to the entry
   * @return null if no entry is found
   */
  private Entry getEntryInTree(final String path) {
    // 1. Search for the parent directory
    final String[] entryNames = StringUtils.split(path, '/');
    if (entryNames.length == 0) {
      return ROOT;
    }
    DirectoryEntry curDirectory = ROOT;
    for (int i = 0; i < entryNames.length-1; i++) {
      final String entryName = entryNames[i];
      boolean childDirectoryFound = false;
      for (final Entry child : curDirectory.getChildren()){
        if (child.getName().equals(entryName) && child.isDirectory()) {
          curDirectory = (DirectoryEntry)child;
          childDirectoryFound = true;
          break;
        }
      }
      if (!childDirectoryFound) {
        return null;
      }
    }

    // 2. Search the parent directory for the file
    for (final Entry child : curDirectory.getChildren()) {
      if (child.getName().equals(entryNames[entryNames.length-1])) {
        return child;
      }
    }

    return null;
  }

  private void addFileMetaToTree(final String path, final FileMeta fileMeta) throws IOException {
    final int index = path.lastIndexOf('/');
    final String parentName = path.substring(0, index);
    final String fileName = path.substring(index+1, path.length());
    final DirectoryEntry parentDirectory = getOrCreateDirectoryRecursively(parentName);
    parentDirectory.addChild(new FileEntry(fileName, parentDirectory, fileMeta));
  }

  private DirectoryEntry getOrCreateDirectoryRecursively(final String path) throws IOException {
    final Entry entry = getEntryInTree(path);
    if (entry != null) {
      if (entry.isDirectory()) {
        return (DirectoryEntry)entry;
      } else {
        throw new IOException("Attempt to create a directory for a path for which a file already exists"); // TODO: replace this with a Surf-specific Thrift exception
      }
    } else {
      return createDirectoryRecursively(path);
    }
  }

  /**
   * First, create a directory in HDFS
   * Second, create a directory in the tree
   */
  private DirectoryEntry createDirectoryRecursively(final String path) throws IOException {
    final boolean baseSuccess = baseFsClient.mkdirs(path);
    if (baseSuccess) {
      final String[] entryNames = StringUtils.split(path, '/');
      DirectoryEntry curDirectory = ROOT;
      int indexForExisting;
      for (indexForExisting = 0; indexForExisting < entryNames.length; indexForExisting++) {
        boolean childDirectoryFound = false;
        for (final Entry child : curDirectory.getChildren()) {
          if (child.getName().equals(entryNames[indexForExisting])) {
            if (child.isDirectory()) {
              curDirectory = (DirectoryEntry) child; // we assume that such directory exists in baseFS (only first-time consistency guarantee)
              childDirectoryFound = true;
              break;
            } else {
              throw new IOException("There is a file with the same name as a subdirectory of the path"); // TODO: replace this with a Surf-specific Thrift exception
            }
          }
        }

        if (!childDirectoryFound) {
          break;
        }
      }

      // recursively mkdirs the rest of the directories
      int indexForToBeCreated;
      for (indexForToBeCreated = indexForExisting; indexForToBeCreated < entryNames.length; indexForToBeCreated++) {
        LOG.log(Level.INFO, "BEFORE " + getTreeString(ROOT));
        final DirectoryEntry childDirectory = new DirectoryEntry(entryNames[indexForToBeCreated], curDirectory);
        curDirectory.addChild(childDirectory);
        curDirectory = childDirectory;
        LOG.log(Level.INFO, "AFTER " + getTreeString(ROOT));
      }
      return curDirectory;
    } else {
      throw new IOException("baseFS.mkdirs returned false");
    }
  }

  private void registerNewFileMeta(final String path, final FileMeta fileMeta) throws IOException {
    // The order of execution is important because addFileMetaToTree can fail
    addFileMetaToTree(path, fileMeta);
    fileIdToFileMeta.put(fileMeta.getFileId(), fileMeta);
  }

  /**
   * For Debugging: getTreeString(ROOT)
   */
  private String getTreeString(final Entry entry) {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(entry.getName());
    if (entry.isDirectory()) {
      final DirectoryEntry directoryEntry = (DirectoryEntry)entry;
      stringBuilder.append("(");
      for (final Entry childEntry : directoryEntry.getChildren()) {
        stringBuilder.append(getTreeString(childEntry));
        stringBuilder.append(", ");
      }
      stringBuilder.append(")");
    }

    return stringBuilder.toString();
  }
}
