package org.apache.reef.inmemory.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.Launch;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class SurfFSWriteITCase {

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static REEF reef;

  private static final byte[] b = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String TESTDIR = "/user/"+System.getProperty("user.name");

  private static final String TESTPATH1 = TESTDIR+"/"+"WRITE.short";
  private static final int SIZE1 = 1;

  private static final String TESTPATH2 = TESTDIR+"/"+"WRITE.long";
  private static final int SIZE2 = 200;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final int BLOCK_SIZE = 800;

  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 800 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    com.microsoft.tang.Configuration clConf = Launch.parseCommandLine(new String[]{"-dfs_address", baseFs.getUri().toString()});
    com.microsoft.tang.Configuration fileConf = Launch.parseConfigFile();
    reef = Launch.runInMemory(clConf, fileConf);

    try {
      Thread.sleep(10000); // Wait for reef setup before continuing
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);

    final FSDataOutputStream stream1 = surfFs.create(new Path(TESTPATH1), true, 4096, (short)3, BLOCK_SIZE);
    for (int i = 0; i < SIZE1; i++) {
      stream1.write(b);
    }
    stream1.close();

    final FSDataOutputStream stream2 = surfFs.create(new Path(TESTPATH2), true, 4096, (short)3, BLOCK_SIZE);
    for (int i = 0; i < SIZE2; i++) {
      stream2.write(b);
    }
    stream2.close();
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    reef.close(); // TODO: does not kill Launchers -- for now, remember to kill from command line
  }

  private FSDataInputStream open(Path path) throws IOException {
    FSDataInputStream in = null;
    in = surfFs.open(path);
    return in;
  }

  private void read(String path, int size) throws IOException {
    FSDataInputStream in = open(new Path(path));

    byte[] readBuf = new byte[size * b.length];

    int bytesRead = in.read(0, readBuf, 0, readBuf.length);
    assertEquals(bytesRead, readBuf.length);
    for (int i = 0; i < size * b.length; i++) {
      assertEquals("At index "+i, b[i % b.length], readBuf[i]);
    }
  }

  @Test
  public void testRead() throws IOException {
    read(TESTPATH1, SIZE1);
    read(TESTPATH2, SIZE2);
  }

  @Test
  public void testExists() throws IOException {
    assertTrue(surfFs.exists(new Path(TESTPATH1)));
    assertTrue(surfFs.exists(new Path(TESTPATH2)));
    assertFalse(surfFs.exists(new Path("Not_exists")));
  }
}