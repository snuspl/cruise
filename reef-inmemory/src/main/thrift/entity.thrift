namespace java org.apache.reef.inmemory.fs.entity

struct BlockInfo {
	1: i64 blockId, //Block unique id
	2: i32 offSet, //order of block
	3: i32 length, //the size of block by byte
	4: list<string> locations //block location. Theses are server ip addresses containig the block.
}

struct User {
	1: string id, //userid
	2: string group //usergroup
}

struct FileMeta{
	1:i64 fileId, //File unique id
	2:string fileName, //file name
	3:string fullPath, //file's abolute path
	4:i64 fileSize, //the size of file by byte
	5:i64 blockSize, //the size of blocks consisting of file.
	6:i64 creationTime, //file creation time
	7:bool directory, // file or directory
	8:list<BlockInfo> blocks, //Block informations consisting of file.
	9:bool complete, //whether the file is complete or not
	10:User owner //Owner information of thie file.
}