include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.fs.service

/**
 * Cache operations supported by the Driver. These are called by the Surf
 * client, in order to satisfy requests made by the App through the FileSystem
 * interface.
 */
service SurfMetaService {
	// List status of files or directories
	list<entity.FileMeta> listStatus(1:string path, 2:bool recursive, 3:entity.User user) throws (1: exceptions.FileNotFoundException fe),
	// Make a new directory
	entity.FileMeta makeDirectory(1:string path, 2:entity.User user) throws (1: exceptions.FileAlreadyExistsException fe)
}
