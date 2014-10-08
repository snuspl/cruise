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
	 * Get the list of blocks and locations for the file
	 */
	entity.FileMeta getFileMeta(1:string path) throws (1: exceptions.FileNotFoundException fe)

  /**
   * Check whether the file exists in the path
   **/
  bool exists(1:string path)

  /**
   * Register FileMeta to enable data access
   */
  bool registerFileMeta(1:entity.FileMeta fileMeta) throws (1: exceptions.FileAlreadyExistsException fae)

  /**
   * Update metadata with a new FileMeta
   */
  bool updateFileMeta(1:entity.FileMeta fileMeta) throws (1: exceptions.FileNotFoundException fe)
}