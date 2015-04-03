package org.apache.reef.inmemory.driver.metatree;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.FileMetaStatus;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.exceptions.FileAlreadyExistsException;
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
  private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock(true); // TODO: replace this with a more fine-grained LOCK

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
   * @param path to the FileMetas
   * @return null if no filmeta for the exact path exists
   */
  public FileMeta getFileMeta(final String path) {
    LOCK.readLock().lock();
    try {
      final Entry entry = getEntryInTree(path);
      if (entry != null && !entry.isDirectory()) {
        return ((FileEntry) entry).getFileMeta();
      } else {
        return null;
      }
    } finally {
      LOCK.readLock().unlock();
    }
  }

  /**
   * List entries at a path
   *
   * @param path to a directory or a file
   * @return null if no such path exists
   */
  public List<FileMetaStatus> listFileMetaStatus(final String path) {
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
        return null;
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
   * Synchronized to avoid creating duplicate files
   */
  public synchronized FileMeta getOrLoadFileMeta(final String path) throws IOException {
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
      addFileMetaToTree(path, fileMeta);
      fileIdToFileMeta.put(fileId, fileMeta);
      return fileMeta;
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  public void createFileInBaseAndTree(final String path, final long blockSize, final short baseFsReplication) throws FileAlreadyExistsException { // TODO: finer-grained throw?
    // Exploit the lease in HDFS to prevent concurrent create(and thus write) of a file
    try {
      baseFsClient.create(path, baseFsReplication, blockSize); // TODO: hold onto the outputstream (close later)
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create a file to the BaseFS : " + path, e.getCause()); // TODO: FileAlreadyExists
      throw new FileAlreadyExistsException(e.getMessage());
    }

    final FileMeta fileMeta = new FileMeta();
    final long fileId = atomicFileId.incrementAndGet();
    fileMeta.setFileId(fileId);
    fileMeta.setFileSize(0);
    fileMeta.setBlockSize(blockSize);
    fileMeta.setBlocks(new ArrayList<BlockMeta>());

    LOCK.writeLock().lock();
    try {
      addFileMetaToTree(path, fileMeta);
      fileIdToFileMeta.put(fileId, fileMeta);
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  /**
   * Add newly written blocks reported by CacheNodes
   */
  public void addNewWrittenBlockToFileMetaInTree(final BlockId blockId, final long nWritten, final CacheNode cacheNode) {
    LOCK.writeLock().lock();
    try {
      final FileMeta fileMeta = fileIdToFileMeta.get(blockId.getFileId());
      if (fileMeta != null) {
        final List<NodeInfo> nodeList = Arrays.asList(new NodeInfo(cacheNode.getAddress(), cacheNode.getRack()));
        final BlockMeta blockMeta = new BlockMeta(blockId.getFileId(), blockId.getOffset(), fileMeta.getBlockSize(), nodeList); // TODO: check replication when we implement replicated write
        fileMeta.setFileSize(fileMeta.getFileSize() + nWritten);
        fileMeta.addToBlocks(blockMeta);
      } else {
        // TODO: we may want to return boolean here for upstream handling
      }
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
      final boolean baseSuccess = baseFsClient.mkdirs(path);
      if (baseSuccess) {
        getOrCreateDirectoryRecursively(path);
        return true;
      } else {
        return false;
      }
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
              srcEntry.rename(dstEntry);
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
    // TODO: make use of recursive
    LOCK.writeLock().lock();
    try {
      final boolean baseFsSuccess = baseFsClient.delete(path);
      if (baseFsSuccess) {
        final Entry entry = getEntryInTree(path);
        if (entry != null) {
          entry.getParent().removeChild(entry);
        } else {
          LOG.log(Level.INFO, "Attempted to delete FileEntry that does not exist in the tree", path);
        }
        return true;
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


  //////// Helper Methods

  /**
   * @param path to the entry
   * @return null if no entry is found
   */
  private Entry getEntryInTree(final String path) {
    // 1. Search for the parent directory
    final String[] entryNames = StringUtils.split(path, '/');
    DirectoryEntry curDirectory = ROOT;
    for (int i = 0; i < entryNames.length-1; i++) { // TODO: Handle cornercase when length==0
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

  private FileMeta addFileMetaToTree(final String path, final FileMeta fileMeta) {
    final int index = path.lastIndexOf('/');
    final String parentName = path.substring(0, index);
    final String fileName = path.substring(index+1, path.length());
    final DirectoryEntry parentDirectory = getOrCreateDirectoryRecursively(parentName);
    parentDirectory.addChild(new FileEntry(fileName, parentDirectory, fileMeta));
    return fileMeta;
  }

  private DirectoryEntry getOrCreateDirectoryRecursively(final String path) {
    final String[] entryNames = StringUtils.split(path, '/');
    DirectoryEntry curDirectory = ROOT;

    for (final String entryName : entryNames) {
      boolean childDirectoryFound = false;
      for (final Entry child : curDirectory.getChildren()) {
        if (child.isDirectory() && child.getName().equals(entryName)) {
          curDirectory = (DirectoryEntry)child;
          childDirectoryFound = true;
          break;
        }
      }

      if (!childDirectoryFound) {
        final DirectoryEntry childDirectory = new DirectoryEntry(entryName, curDirectory);
        curDirectory.addChild(childDirectory);
        curDirectory = childDirectory;
      }
    }
    return curDirectory;
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
