namespace java org.apache.reef.inmemory.fs.entity

/*
 * Contains relevant block information to be stored as Metadata.
 * Currently takes after o.a.h.hdfs.protocol.ExtendedBlock
 */
struct BlockInfo {
	1: i64 blockId,            // Block id (unique)
	2: i64 offSet,             // Order of the block
	3: i64 length,             // Size of the block in bytes
	4: list<string> locations, // Block location. These are server ip addresses containing the block.
	5: string namespaceId,     // The namespace, e.g. HDFS block pool ID
	6: i64 generationStamp,    // Version number for append-able FSes, e.g. HDFS (set to 0 when not append-able)
	7: string token            // Token
}

struct User {
	1: string id,             // User id
	2: string group           // User group
}

struct FileMeta{
	1:i64 fileId,             // File id (unique)
	2:string fileName,        // File name
	3:string fullPath,        // File's abolute path
	4:i64 fileSize,           // Size of the file in bytes
	5:i64 blockSize,          // Size of blocks consisting of the file.
	6:i64 creationTime,       // File creation time
	7:bool directory,         // Whether the file is a file or directory.
	8:list<BlockInfo> blocks, // Information of blocks consisting of the file.
	9:bool complete,          // Whether the file is complete or not
	10:User owner             // Owner information of the file.
}

struct CacheData {
    1:i64 length,
    2:binary data
}
