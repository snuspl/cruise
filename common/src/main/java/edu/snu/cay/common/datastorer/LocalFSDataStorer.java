package edu.snu.cay.common.datastorer;

import edu.snu.cay.common.datastorer.param.BaseDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements a DataStore that stores data to local file system.
 */
public class LocalFSDataStorer implements DataStorer {
  private static final Logger LOG = Logger.getLogger(LocalFSDataStorer.class.getName());
  private final Path baseDir;

  @Inject
  private LocalFSDataStorer(@Parameter(BaseDir.class) final String baseDir) {
    this.baseDir = new Path(baseDir);
  }

  @Override
  public void storeData(final String subPathStr, final byte[] data) throws IOException {
    final Path path = new Path(baseDir, subPathStr);
    final FileSystem fs = LocalFileSystem.get(new Configuration());

    try (final FSDataOutputStream fos = fs.create(path)) {
      fos.write(data);
      fos.close();
    }

    LOG.log(Level.INFO, "Successfully wrote {0} bytes data to {1}", new Object[] {data.length, path.toString()});
  }
}
