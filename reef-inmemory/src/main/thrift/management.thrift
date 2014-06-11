include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.cache.service

service SurfManagementService {
    i64 clear()
}
