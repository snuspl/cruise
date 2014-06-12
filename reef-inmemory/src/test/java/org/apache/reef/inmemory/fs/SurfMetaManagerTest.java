package org.apache.reef.inmemory.fs;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.User;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Test class for SurfMeta
 */
public class SurfMetaManagerTest extends TestCase {
  SurfMetaManager metaManager;
  User user;

  @Override
  public void setUp() {
    metaManager = new SurfMetaManager();
    user = new User();
    user.setId("surf");
    user.setGroup("surf");
  }

  @Test
  public void testGetNotExists() throws FileNotFoundException {
    List<BlockInfo> blocks = null;
    try {
      blocks = metaManager.getBlocks(new Path("/path/not/found"), new User());
      assertTrue("Expected but did not receive exception", false);
    } catch (FileNotFoundException e) {
      assertTrue(true); // Test passes
    } catch (Throwable t) {
      assertTrue("Unexpected throwable "+t, false);
    }
    assertNull("Received blocks that do not exist", blocks);
  }

  @Test
  public void testClear() {
    // TODO: Fill in metadata first, once that functionality is created
    assertEquals(0, metaManager.clear());
  }
}
