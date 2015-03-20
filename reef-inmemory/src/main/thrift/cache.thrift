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
    binary getData(1:entity.BlockMeta block, 2:i64 offset, 3:i64 length)
        throws (1: exceptions.BlockNotFoundException nfe, 2:exceptions.BlockLoadingException le)

    /**
     * Initialize a block in the cache to write data. Client should call this
     * before sending the data through write() once he/she has allocated a block from MetaServer.
     */
    void initBlock(1:string path, 2:i64 offset, 3:i64 blockSize, 4:entity.AllocatedBlockMeta info)

    /**
     * Receive data packet from the Client and write it into the cache.
     * blockOffset : Start offset of file associated with this block
     * innerOffset : Start offset of block associated with ths packet
     */
    void writeData(1:string path, 2:i64 blockOffset, 3:i64 blockSize, 4:i64 innerOffset, 5:binary buf, 6:bool isLastPacket)
}