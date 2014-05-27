namespace java org.apache.reef.inmemory.fs.entity

struct BlockInfo {
	1: i64 blockId,
	2: i32 offSet,
	3: i32 length,
	4: list<string> locations
}

struct User {
	1: string id,
	2: string group
}

struct FileMeta{
	1:i64 fileId,
	2:string fileName,
	3:string fullPath,
	4:i64 fileSize,
	5:i64 blockSize,
	6:i64 creationTime,
	7:bool directory,
	8:list<BlockInfo> blocks,
	9:bool complete,
	10:User owner
}