include "entity.thrift"
include "exceptions.thrift"

namespace java org.apache.reef.inmemory.fs.service

service SurfMetaService {
  //list statuses of file or directories
	list<entity.FileMeta> listStatus(1:string path, 2:bool recursive, 3:entity.User user) throws (1: exceptions.FileNotFoundException fe),
	//make a new directory
	entity.FileMeta makeDirectory(1:string path, 2:entity.User user) throws (1: exceptions.FileAlreadyExistsException fe)
}