include "exceptions.thrift"

namespace java org.apache.reef.inmemory.common.service

/**
 * Management operations supported by the Driver. The management CLI is used
 * to call these.
 */
service SurfManagementService {
    string getStatus()
    i64 clear()
    bool load(1:string path) throws (1: exceptions.FileNotFoundException ex)
    /**
     * Add a new cache node. To use default Surf memory size, specify memory as 0.
     */
    string addCacheNode(1:i32 memory) throws (1: exceptions.AllocationFailedException afe,
                                              2: exceptions.SubmissionFailedException sfe)
}
