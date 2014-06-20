package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.reef.inmemory.fs.service.SurfMetaService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

public class CachedFS extends FileSystem {

  private static final Logger LOG = Logger.getLogger(CachedFS.class.getName());

  // These cannot be final, because the empty constructor is expected
  FileSystem baseFs;
  SurfMetaService.Client thriftClient;

  URI uri;
  URI hdfsUri;


  public CachedFS() {
  }

  protected CachedFS(final FileSystem baseFs,
                  final SurfMetaService.Client thriftClient) {
    this.baseFs = baseFs;
    this.thriftClient = thriftClient;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    this.uri = uri;
    this.hdfsUri = URI.create("hdfs://"+uri.getAuthority());

    this.baseFs = new DistributedFileSystem();
    this.baseFs.initialize(this.hdfsUri, conf);
  }

  @Override
  public String getScheme() {
    return "surf";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, final int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i2, long l, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path path, Path path2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    return baseFs.listStatus(path);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    return baseFs.getWorkingDirectory(); // TODO: does this make sense?
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return baseFs.getFileStatus(path);
  }
}
