namespace java org.apache.reef.inmemory.common.entity

/**
 * Contains information about cache server location and rack
 */
struct NodeInfo {
    1: string address,               // server host:port
    2: string rack                   // rack where address is located, used for locality
}

/**
 * Contains relevant block information to be stored as Metadata.
 * Currently takes after o.a.h.hdfs.protocol.ExtendedBlock
 */
struct BlockInfo {
	1: string filePath,              // File's absolute path
	2: i64 blockId,                  // Block id (unique)
	3: i64 offSet,                   // Order of the block
	4: i64 length,                   // Size of the block in bytes
	5: list<NodeInfo> locations,     // Block locations. Metaserver should return a sorted list according to locality.
	6: string namespaceId,           // The namespace, e.g. HDFS block pool ID
	7: i64 generationStamp,          // Version number for append-able FSes, e.g. HDFS (set to 0 when not append-able)
	8: string token                  // Token
}

/**
 * Contains block information of newly allocated from the MetaServer
 * as a response for request of AllocateBlock
 */
struct AllocatedBlockInfo {
  1: list<NodeInfo> locations,      // Cache locations including the nodes to replicate
  2: bool pin,                      // True if this block is to be pinned
  3: i32 baseReplicationFactor,     // Replication factor to base File System.
  4: bool writeThrough              // True if the synchronization method is Write-through
}

/**
 * TODO: revisit when implementing ls commands at Surf Driver
 */
struct User {
	1: string id,             // User id
	2: string group           // User group
}

/**
 * TODO: revisit when implementing ls commands at Surf Driver
 */
struct FileMeta{
	1:i64 fileId,             // File id (unique)
	2:string fileName,        // File name
	3:string fullPath,        // File's absolute path
	4:i64 fileSize,           // Size of the file in bytes
	5:i64 blockSize,          // Size of blocks consisting of the file.
	6:i64 creationTime,       // File creation time
	7:bool directory,         // Whether the file is a file or directory.
	8:list<BlockInfo> blocks, // Information of blocks consisting of the file.
	9:bool complete,          // Whether the file is complete or not
	10:User owner             // Owner information of the file.
}
