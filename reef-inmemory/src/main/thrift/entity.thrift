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
 * This data structure coincide to FileMeta, which is created/managed in Surf.
 */
struct BlockMeta {
	1: string filePath,              // File's absolute path TODO Replace filePath with another unique field (e.g. fileId)
	2: i64 offSet,                   // Order of the block
	3: i64 length,                   // Size of the block in bytes
	4: list<NodeInfo> locations,     // Block locations. Metaserver should return a sorted list according to locality.
}

/**
 * Contains block information of newly allocated from the MetaServer
 * as a response for request of AllocateBlock
 * TODO This will be integrated into BlockMeta
 */
struct AllocatedBlockMeta {
  1: list<NodeInfo> locations,      // Cache locations including the nodes to replicate
  2: bool pin,                      // True if this block is to be pinned
  3: i32 baseReplicationFactor,     // Replication factor to base File System.
  4: bool writeThrough              // True if the synchronization method is Write-through
}

/**
 * TODO: Include Permission information
 */
struct User {
  1: string owner,          // User
  2: string group           // User group
}

struct FileMeta{
	1:string fullPath,        // File's absolute path TODO Replace filePath with another unique field (e.g. fileId)
	2:i64 fileSize,           // Size of the file in bytes
	3:bool directory,         // Whether the file is a file or directory.
	4:i16 replication,        // Replication status of the file.
	5:i64 blockSize,          // Size of blocks consisting of the file.
	6:i64 modificationTime,   // File modification time
	7:i64 accessTime,         // File access time
	8:list<BlockMeta> blocks, // Information of blocks consisting of the file.
	9:User user,              // Access information of the file.
	10:string symLink,        // SymLink information of the file.
	11:list<string> children,	// Children paths in case of directory. TODO Remove when we have a metadata tree
}
