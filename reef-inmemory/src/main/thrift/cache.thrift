include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.fs.service

/**
 * Cache retrieval from Tasks. These are called by the Surf client to
 * get data from each Task node.
 */
service SurfCacheService {
    binary getData(1:entity.BlockInfo block) throws (1: exceptions.BlockNotFoundException nfe, 2:exceptions.BlockLoadingException le)
}