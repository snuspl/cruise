package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for default implementation of load progress manager
 */
public final class LoadProgressManagerImplTest {

  public static final String[] hostArr = new String[]{"hostA", "hostB", "hostC"};

  public List<NodeInfo> hosts;
  private Configuration conf;

  @Before
  public void setUp() {
    hosts = new ArrayList<>(hostArr.length);
    for (String host : hostArr) {
      hosts.add(new NodeInfo(host, "/test-rack"));
    }
    conf = new Configuration();
  }

  /**
   * Test that caches that are not connected get removed after max number of tries
   */
  @Test
  public void testNotConnected() throws IOException {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, conf);

    for (int i = 0; i < hosts.size(); i++) {
      for (int j = 0; j < LoadProgressManagerImpl.LOAD_MAX_NOT_CONNECTED_DEFAULT; j++) {
        final String host = progressManager.getNextCache();
        assertNotNull(host);
        progressManager.notConnected(host);
      }
    }

    try {
      final String host = progressManager.getNextCache();
      fail("Expected no more candidates");
    } catch (IOException e) {
      // expected
    }
  }

  /**
   * Test that caches where files cannot be found are removed after max number of tries
   */
  @Test
  public void testNotFound() throws IOException {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, conf);

    for (int i = 0; i < hosts.size(); i++) {
      for (int j = 0; j < LoadProgressManagerImpl.LOAD_MAX_NOT_FOUND_DEFAULT; j++) {
        final String host = progressManager.getNextCache();
        assertNotNull(host);
        progressManager.notFound(host);
      }
    }
    try {
      final String host = progressManager.getNextCache();
      fail("Expected no more candidates");
    } catch (IOException e) {
      // expected
    }

  }

  /**
   * Test that cache showing OK loading progress does not get switched to another cache
   */
  @Test
  public void testOKLoadingProgress() throws InterruptedException, IOException {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, conf);

    // When progress is OK, continue using same host
    String prevHost = null;
    for (int j = 0; j < 10; j++) {
      final String host = progressManager.getNextCache();
      assertNotNull(host);

      Thread.sleep(100);
      progressManager.loadingProgress(host, (j+1) * LoadProgressManagerImpl.LOAD_OK_PROGRESS_DEFAULT); // 10 * OK

      if (prevHost != null) {
        assertEquals(prevHost, host);
      }
      prevHost = host;
    }
  }

  /**
   * Test that caches showing not OK (but better than none) progress gets switched to another cache
   */
  @Test
  public void testNotOKLoadingProgress() throws InterruptedException, IOException {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, conf);

    // When progress is less than OK but not NONE, switch host
    String prevHost = null;
    for (int j = 0; j < 10; j++) {
      final String host = progressManager.getNextCache();
      assertNotNull(host);

      Thread.sleep(100);
      final long bytesLoaded = (long) (1.0 / 10.0 * (j+1) *
                    (LoadProgressManagerImpl.LOAD_NO_PROGRESS_DEFAULT +
                            LoadProgressManagerImpl.LOAD_OK_PROGRESS_DEFAULT) / 2); // Halfway between NO and OK
      progressManager.loadingProgress(host, bytesLoaded);

      if (prevHost != null) {
        assertNotEquals("Values should be different, iteration "+j, prevHost, host);
      }
      prevHost = host;
    }
  }
}
