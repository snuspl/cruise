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
	entity.FileMeta getFileMeta(1:string path, 2:string clientHostname) throws (1: exceptions.FileNotFoundException fe)
}
