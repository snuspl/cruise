package org.apache.reef.inmemory.cache.service;

import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SurfCacheServerTest {

  @Test
  public void testFindEphemeralPort() throws IOException {
    SurfCacheServer cacheServer = new SurfCacheServer(null, 0, 0, 1);

    int bindPort = cacheServer.initBindPort();
    assertEquals(bindPort, cacheServer.getBindPort());
    assertNotEquals(0, cacheServer.getBindPort());

    SurfCacheServer secondServer = new SurfCacheServer(null, 0, 0, 1);

    // Should not immediately give back the same port
    int secondPort = secondServer.initBindPort();
    assertEquals(secondPort, secondServer.getBindPort());
    assertNotEquals(0, secondServer.getBindPort());
    assertNotEquals(cacheServer.getBindPort(), secondServer.getBindPort());

    // Reuse port; should not throw Exception
    ServerSocket socket = new ServerSocket(secondServer.getBindPort());
    socket.close();
  }
}
