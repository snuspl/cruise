include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.common.service

/**
 * Cache operations supported by the Driver. These are called by the Surf
 * client, in order to satisfy requests made by the App through the FileSystem
 * interface.
 */
service SurfMetaService {
  /**
   * Get the list of blocks and locations for the file.
   * The returned list will be sorted by locality w.r.t. clientHostname.
   */
  entity.FileMeta getFileMeta(1:string path, 2:string clientHostname) throws (1: exceptions.FileNotFoundException fnfe)

  /**
   * Check whether the file exists in the path
   */
  bool exists(1:string path)

  /**
   * Register the Filemeta with given path and blockSize
   */
  bool create(1:string path, 2:i16 replication, 3:i64 blockSize) throws (1: exceptions.FileAlreadyExistsException fae)

  /**
   * Register the FileMeta and insert an entry to directory structure if successful.
   */
  bool mkdirs(1:string path) throws (1: exceptions.FileAlreadyExistsException fae)

  /**
   * List the statuses of the files/directories
   **/
  list<entity.FileMeta> listMeta(1:string path) throws (1: exceptions.FileNotFoundException fnfe)

  /**
   * Allocate a new Block and return the Block's Metadata (CacheNode, Replication Policy, etc) of the block to write data.
   */
  entity.AllocatedBlockMeta allocateBlock(1:string path, 2:i64 offset, 3:string clientAddress)

  /**
   * Announce to the Meta server that the file is complete
   */
  bool completeFile(1:string path, 2:i64 fileSize)
}
