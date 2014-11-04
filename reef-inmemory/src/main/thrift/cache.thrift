include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.common.service

/**
 * Cache retrieval from Tasks. These are called by the Surf client to
 * get data from each Task node.
 */
service SurfCacheService {
    /**
     * Transfer block data from Task to Client
     */
    binary getData(1:entity.BlockInfo block, 2:i64 offset, 3:i64 length)
        throws (1: exceptions.BlockNotFoundException nfe, 2:exceptions.BlockLoadingException le)

    /**
     * Initialize a block in the cache to write data. Client should call this
     * before sending the data through write() once he/she has allocated a block from MetaServer.
     */
    void initBlock(1:string path, 2:i64 offset, 3:i64 blockSize, 4:entity.AllocatedBlockInfo info)

    /**
     * Receive data from the Client and write it into the cache.
     */
    void writeData(1:string path, 2:i64 blockOffset, 3:i64 blockSize, 4:i64 innerOffset, 5:binary buf)
}