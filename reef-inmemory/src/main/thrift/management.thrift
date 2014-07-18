include "exceptions.thrift"

namespace java org.apache.reef.inmemory.common.service

/**
 * Management operations supported by the Driver. The management CLI is used
 * to call these.
 */
service SurfManagementService {
    /**
     * Get the status of Cache Nodes
     */
    string getStatus()

    /**
     * Clear blocks from all Caches
     */
    i64 clear()

    /**
     * Load path into Surf
     */
    bool load(1:string path) throws (1: exceptions.FileNotFoundException ex)

    /**
     * Add a new cache node. To use default Surf memory size, specify memory as 0.
     */
    string addCacheNode(1:i32 memory)
}
