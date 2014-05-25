package org.apache.reef.inmemory.fs;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

/**
 * Test class for SurfMeta
 */
public class SurfMetaManagerTest extends TestCase {
  SurfMetaManager sm;
  org.apache.reef.inmemory.fs.entity.User user;

  @Override
  public void setUp() {
    sm = new SurfMetaManager();
    user = new org.apache.reef.inmemory.fs.entity.User();
    user.setId("surf");
    user.setGroup("surf");
  }

  @Test
  public void testMakeDirectory() throws FileAlreadyExistsException {
    //Absolute path creating test
    org.apache.reef.inmemory.fs.entity.FileMeta fm = sm.makeDirectory(new Path("/user/hive/home"), user);
    assertEquals("Directory name is different.", fm.getFullPath(), "/user/hive/home");

    try {
      sm.makeDirectory(new Path("/user/hive"), user);
      assertFalse("Directory /user/hive already exists", true);
    } catch (FileAlreadyExistsException fe) {
      assertTrue(true);
    }

    //Relative path creating test
    fm = sm.makeDirectory(new Path("hive"), user);
    assertEquals("Directory name is different.", "/user/surf/hive", fm.getFullPath());
  }

  @Test
  public void testListStatus() throws FileNotFoundException, FileAlreadyExistsException {
    List<org.apache.reef.inmemory.fs.entity.FileMeta> fms = sm.listStatus(new Path("/user/hive"), false, user);
    assertEquals("Directory name is different", "/user/hive/home", fms.get(0).getFullPath());

    try {
      fms = sm.listStatus(new Path("/user/test"), false, user);
      assertFalse("FileNotFoundException is expected", true);
    } catch (FileNotFoundException fe) {
      assertTrue(true);
    }

    sm.makeDirectory(new Path("test"), user);

    fms = sm.listStatus(new Path("./"), false, user);
    assertEquals("Sub directories count is different", 2, fms.size());

    fms = sm.listStatus(new Path("/"), false, user);
    assertEquals("Sub directories count is different", 2, fms.size());
  }
}
