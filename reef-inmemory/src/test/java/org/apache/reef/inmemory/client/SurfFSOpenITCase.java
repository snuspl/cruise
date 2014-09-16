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
import org.apache.hadoop.io.IOUtils;
import org.apache.reef.inmemory.Launch;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for SurfFS methods that delegate to a Base FS.
 * The tests use HDFS as the Base FS, by connecting to a base HDFS minicluster
 *
 * This end-to-end test is currently set to be ignored. The reason is that,
 * although the test executes fine, it does not cleanup the Processes created.
 * TODO: When REEF issue #868 is available, call close using that method
 */
@Ignore
public final class SurfFSOpenITCase {

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static REEF reef;

  private static final byte[] b = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String TESTDIR = "/user/"+System.getProperty("user.name");

  private static final String TESTPATH1 = TESTDIR+"/"+"COUNT.short";
  private static final int SIZE1 = 1;

  private static final String TESTPATH2 = TESTDIR+"/"+"COUNT.long";
  private static final int SIZE2 = 70;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:9001";

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   * Launch REEF instance.
   */
  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 512);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);

    baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    com.microsoft.tang.Configuration clConf = Launch.parseCommandLine(new String[]{"-dfs_address", baseFs.getUri().toString()});
    com.microsoft.tang.Configuration fileConf = Launch.parseConfigFile();
    reef = Launch.runInMemory(clConf, fileConf);

    final FSDataOutputStream stream1 = baseFs.create(new Path(TESTPATH1));
    for (int i = 0; i < SIZE1; i++) {
      stream1.write(b);
    }
    stream1.close();

    final FSDataOutputStream stream2 = baseFs.create(new Path(TESTPATH2));
    for (int i = 0; i < SIZE2; i++) {
      stream2.write(b);
    }
    stream2.close();

    try {
      Thread.sleep(10000); // Wait for reef setup before continuing
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    conf.setInt(SurfFS.CACHECLIENT_BUFFER_SIZE_KEY, 64);

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF+"://"+SURF_ADDRESS), conf);
  }

  /**
   * Remove all directories.
   * Shutdown REEF.
   */
  @AfterClass
  public static void tearDownClass() throws IOException {
    baseFs.delete(new Path("/"), true);
    System.out.println("Closing REEF...");
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

  private void copyBytes(String path, int size) throws IOException {
    FSDataInputStream in = open(new Path(path));
    OutputStream out = new ByteArrayOutputStream(size * b.length);

    IOUtils.copyBytes(in, out, size * b.length);
  }

  @Test
  public void testRead() throws IOException {
    read(TESTPATH1, SIZE1);
    read(TESTPATH2, SIZE2);
    // TODO: Check various boundary conditions
  }

  @Test
  public void testCopyBytes() throws IOException {
    copyBytes(TESTPATH1, SIZE1);
    copyBytes(TESTPATH2, SIZE2);
    // TODO: Check various boundary conditions
  }

  @Test
  public void testSeek() throws IOException {
    FSDataInputStream in = open(new Path(TESTPATH2));

    assertEquals((byte) 1, in.readByte());
    in.seek(1);
    assertEquals((byte) 2, in.readByte());
    in.seek(511);
    assertEquals((byte) 8, in.readByte());
    in.seek(8);
    assertEquals((byte) 1, in.readByte());
    in.seek(512);
    assertEquals((byte) 1, in.readByte());
    try {
      in.seek(1024);
      fail("Should return EOF");
    } catch (EOFException e) {
      // passed
    } catch (Exception e) {
      fail("Should return EOF, instead returned "+e);
    }
    in.seek(518);
    assertEquals((byte) 7, in.readByte());
    in.seek(2);
    assertEquals((byte) 3, in.readByte());
  }
}
