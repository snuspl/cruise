package org.apache.reef.inmemory.fs;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.User;
import org.junit.Test;

import java.io.FileNotFoundException;

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

  @Test(expected= FileNotFoundException.class)
  public void testGetNotExists() throws FileNotFoundException {
    metaManager.getBlocks(new Path("/path/not/found"), new User());
  }

  @Test
  public void testClear() {
    // TODO: Fill in metadata first, once that functionality is created
    assertEquals(0, metaManager.clear());
  }
}
