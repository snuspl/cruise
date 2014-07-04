include "exceptions.thrift"

namespace java org.apache.reef.inmemory.fs.service

/**
 * Management operations supported by the Driver. The management CLI is used
 * to call these.
 */
service SurfManagementService {
    i64 clear()
    bool load(1:string path) throws (1: exceptions.FileNotFoundException ex)
    string addCacheNode(1:string host, 2:i32 port) throws (1: exceptions.AllocationFailedException afe,
                                                           2: exceptions.SubmissionFailedException sfe)
}
