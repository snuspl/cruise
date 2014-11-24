package org.apache.reef.inmemory.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.Launch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.fail;
@Ignore
public class SurfFSCreateITCase {

  private static SurfFS surfFs;

  private static REEF reef;

  private static final String TESTDIR = "/user/"+System.getProperty("user.name");

  private static final String TESTPATH1 = TESTDIR+"/"+"CREATE.unclosed";
  private static final String TESTPATH2 = TESTDIR+"/"+"CREATE.closed";


  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   */
  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException, InterruptedException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);

    com.microsoft.tang.Configuration clConf = Launch.parseCommandLine(new String[]{});
    com.microsoft.tang.Configuration fileConf = Launch.parseConfigFile();
    reef = Launch.runInMemory(clConf, fileConf);
    try {
      Thread.sleep(3000); // Wait for reef setup before continuing
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final Configuration conf = new Configuration();

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF+"://"+SURF_ADDRESS), conf);
    System.out.println("SurfFs address resolved to:"+SURF+"://"+SURF_ADDRESS );
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    System.out.println("Closing REEF...");
    reef.close(); // TODO: does not kill Launchers -- for now, remember to kill from command line
  }

  public FSDataOutputStream create(Path path) throws IOException {
    FSDataOutputStream out = null;
    out = surfFs.create(path);
    return out;
  }

  private FSDataInputStream open(Path path) throws IOException {
    FSDataInputStream in = null;
    in = surfFs.open(path);
    return in;
  }

  @Test
  public void testOutputStreamNotClosed() throws IOException {
    create(new Path(TESTPATH1));

    // CASE 1: create
    try {
      create(new Path(TESTPATH1));
      fail("Should return IOException. Because the file exists");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should return IOException, instead returned "+e);
    }

    // CASE 2: open the file. It is possible to access the file as soon as it is created.
    open(new Path(TESTPATH1));
  }

  @Test
  public void testOutputStreamClosed() throws IOException {
    FSDataOutputStream out1 = create(new Path(TESTPATH2));
    out1.close();

    // CASE 1: create a file with the same name
    try {
      create(new Path(TESTPATH2));
      fail("Should return IOException. Because the file exists");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should return IOException, instead returned "+e);
    }

    // CASE 2: open the file
    open(new Path(TESTPATH2));
  }
}