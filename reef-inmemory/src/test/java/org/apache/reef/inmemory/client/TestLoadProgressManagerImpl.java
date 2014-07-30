package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public final class TestLoadProgressManagerImpl {

  public static final String[] hostArr = new String[]{"hostA", "hostB", "hostC"};
  public static final int length = 128 * 1024 * 1024;

  public List<NodeInfo> hosts;

  @Before
  public void setUp() {
    hosts = new ArrayList<>(hostArr.length);
    for (String host : hostArr) {
      hosts.add(new NodeInfo(host, "/test-rack"));
    }
  }

  @Test
  public void testNotConnected() {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, length);

    for (int i = 0; i < hosts.size(); i++) {
      for (int j = 0; j < LoadProgressManagerImpl.MAX_NOT_CONNECTED; j++) {
        final String host = progressManager.getNextCache();
        assertNotNull(host);
        progressManager.notConnected(host);
      }
    }
    final String host = progressManager.getNextCache();
    assertNull(host);
  }

  @Test
  public void testNotFound() {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, length);

    for (int i = 0; i < hosts.size(); i++) {
      for (int j = 0; j < LoadProgressManagerImpl.MAX_NOT_FOUND; j++) {
        final String host = progressManager.getNextCache();
        assertNotNull(host);
        progressManager.notFound(host);
      }
    }
    final String host = progressManager.getNextCache();
    assertNull(host);
  }

  @Test
  public void testOKLoadingProgress() throws InterruptedException {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, length);

    // When progress is OK, continue using same host
    String prevHost = null;
    for (int j = 0; j < 10; j++) {
      final String host = progressManager.getNextCache();
      assertNotNull(host);

      Thread.sleep(100);
      progressManager.loadingProgress(host, (j+1) * 2 * 1024 * 1024); // about 20 Mbps

      if (prevHost != null) {
        assertEquals(prevHost, host);
      }
      prevHost = host;
    }
  }

  @Test
  public void testNotOKLoadingProgress() throws InterruptedException {
    final LoadProgressManager progressManager = new LoadProgressManagerImpl();
    progressManager.initialize(hosts, length);

    // When progress is less than OK but not NONE, switch host
    String prevHost = null;
    for (int j = 0; j < 10; j++) {
      final String host = progressManager.getNextCache();
      assertNotNull(host);

      Thread.sleep(100);
      progressManager.loadingProgress(host, (j+1) * 2 * 1024); // about 20 Kbps

      if (prevHost != null) {
        assertNotEquals("Values should be different, iteration "+j, prevHost, host);
      }
      prevHost = host;
    }
  }
}
