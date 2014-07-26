package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class SurfFSLocationTest {
  private static FileSystem surfFs;
  private static final int port = 5000;
  private static final long blockLength = 512;
  private static final long modTime = 1406189420000L;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:9001";

  private static final int numBlocks = 5;
  private static final int numLocations = 3;
  private static final long len = blockLength * numBlocks;
  private static final String pathString = "/path/of/test";

  final Path path = new Path(pathString);
  final FileStatus fileStatus = new FileStatus(
          blockLength * numBlocks, false, numLocations, blockLength, modTime, path);

  @Before
  public void setUp() throws IOException, TException {

    final FileMeta fileMeta = new FileMeta();

    for (int i = 0; i < numBlocks; i++) {
      final BlockInfo blockInfo = new BlockInfo();
      blockInfo.setOffSet(blockLength * i);
      blockInfo.setLength(blockLength);
      for (int j = 0; j < numLocations; j++) {
        blockInfo.addToLocations("location-" + i + "-" + j + ":" + port);
      }
      fileMeta.addToBlocks(blockInfo);
    }
    fileMeta.setFullPath(pathString);
    fileMeta.setFileSize(len);

    final SurfMetaService.Client metaClient = mock(SurfMetaService.Client.class);
    when(metaClient.getFileMeta(anyString())).thenReturn(fileMeta);

    final Configuration conf = new Configuration();
    surfFs = new SurfFS(mock(FileSystem.class), metaClient);
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);
  }

  private void assertLocationsCorrect(final BlockLocation blockLocation, final String prefix, long start, long len) throws IOException {
    final String[] names = blockLocation.getNames();
    final String[] hosts = blockLocation.getHosts();

    assertEquals(numLocations, names.length);
    for (int j = 0; j < numLocations; j++) {
      assertEquals(prefix+"-"+j+":"+port, names[j]);
    }
    assertEquals(numLocations, hosts.length);
    for (int j = 0; j < numLocations; j++) {
      assertEquals(prefix+"-"+j, hosts[j]);
    }

    assertEquals(start, blockLocation.getOffset());
    assertEquals(len, blockLocation.getLength());
  }

  @Test
  public void testWholeFile() throws TException, IOException {
    final BlockLocation[] blockLocations = surfFs.getFileBlockLocations(fileStatus, 0, len);

    assertEquals(numBlocks, blockLocations.length);
    for (int i = 0; i < numBlocks; i++) {
      final BlockLocation blockLocation = blockLocations[i];
      assertLocationsCorrect(blockLocation, "location-"+i, blockLength * i, blockLength);
    }
  }

  @Test
  public void testFirstBlockOnly() throws IOException {
    final BlockLocation[] blockLocations = surfFs.getFileBlockLocations(fileStatus, blockLength/2, blockLength/4);

    assertEquals(1, blockLocations.length);
    final BlockLocation blockLocation = blockLocations[0];
    assertLocationsCorrect(blockLocation, "location-"+0, 0, blockLength);
  }

  @Test
  public void testSecondBlockOnly() throws IOException {
    final BlockLocation[] blockLocations = surfFs.getFileBlockLocations(fileStatus, blockLength + blockLength/2, blockLength/4);

    assertEquals(1, blockLocations.length);
    final BlockLocation blockLocation = blockLocations[0];
    assertLocationsCorrect(blockLocation, "location-"+1, blockLength, blockLength);
  }

  @Test
  public void testFinalBlockOnly() throws IOException {
    final BlockLocation[] blockLocations = surfFs.getFileBlockLocations(fileStatus,
            blockLength * (numBlocks-1) + blockLength/2, blockLength/4);

    assertEquals(1, blockLocations.length);
    final BlockLocation blockLocation = blockLocations[0];
    assertLocationsCorrect(blockLocation, "location-"+(numBlocks-1), blockLength * (numBlocks-1), blockLength);
  }

  @Test
  public void testMultipleBlocks() throws IOException {
    final BlockLocation[] blockLocations = surfFs.getFileBlockLocations(fileStatus,
            blockLength + blockLength/3, blockLength * 2); // index 1, 2, 3

    assertEquals(3, blockLocations.length);
    for (int i = 1; i <= 3; i++) {
      final BlockLocation blockLocation = blockLocations[i-1];
      assertLocationsCorrect(blockLocation, "location-"+i, blockLength * i, blockLength);
    }
  }
}
