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
        throws (1: exceptions.BlockNotFoundException nfe, 2:exceptions.BlockLoadingException le,
        3:exceptions.BlockWritingException we)

    /**
     * Initialize a block in the cache to write data. Client should call this
     * before sending the data through write() once he/she has allocated a block from MetaServer.
     */
    void initBlock(1:i64 blockSize, 2:entity.WriteableBlockMeta info)

    /**
     * Receive data packet from the Client and write it into the cache.
     */
    void writeData(1:i64 fileId, 2:i64 blockOffset, 3:i64 innerOffset, 4:binary buf, 5:bool isLastPacket)
}