namespace java org.apache.reef.inmemory.common.entity

/**
 * Contains information about CacheNode location and rack
 */
struct NodeInfo {
  1: string address,               // server host:port
  2: string rack                   // rack where address is located, used for locality
}

/**
  * The key for cached data on the CacheNode-side
  */
struct BlockMeta {
	1: i64 fileId,                   // The Id of the FileMeta this BlockMeta belongs to
	2: i64 offSet,                   // Offset of this block in the file
	3: i64 length,                   // Size of the block in bytes
	4: list<NodeInfo> locations,     // Locations of CacheNode(s) that have this block
}

/**
  * Metadata for data cached in Surf.
  * Independent from data in HDFS
  * Independent from path and policies(replication, pin, etc)
  */
struct FileMeta{
	1: i64 fileId,                    // Unique file id
	2: i64 fileSize,                  // Size of the file in bytes
	3: i64 blockSize,                 // Size of blocks consisting of the file.
	4: list<BlockMeta> blocks,        // Information of blocks consisting of the file.
}

/**
 * Client gets this from Driver to write to CacheNode
 * TODO: info such as baseReplicationFactor, writeThrough will also be needed in the future
 */
struct WriteableBlockMeta {
  1: BlockMeta blockMeta,           // Meta of the block to write
  2: bool pin,                      // Pin policy for this block
  3: i16 replication,               // Replication policy for this block
}

/**
 * Client gets this from Driver and converts it into FileStatus
 */
struct FileMetaStatus {
  1: string path
  2: i64 length,
  3: bool isdir,
  4: i16 block_replication,
  5: i64 blocksize,
  6: i64 modification_time,
  7: i64 access_time,
  8: i16 permisison,
  9: string owner,
  10: string group,
  11: string symlink,
}
