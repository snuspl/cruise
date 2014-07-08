include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.common.service

/**
 * Cache retrieval from Tasks. These are called by the Surf client to
 * get data from each Task node.
 */
service SurfCacheService {
    binary getData(1:entity.BlockInfo block, 2:i64 offset, 3:i64 length)
        throws (1: exceptions.BlockNotFoundException nfe, 2:exceptions.BlockLoadingException le)
}