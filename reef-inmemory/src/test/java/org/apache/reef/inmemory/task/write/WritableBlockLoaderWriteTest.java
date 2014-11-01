package org.apache.reef.inmemory.task.write;

import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test WritableBlockLoader.
 * Initiate the blockLoader with blockSize 16, bufferSize 4
 */
public class WritableBlockLoaderWriteTest {
  BlockId id;
  WritableBlockLoader loader;

  @Before
  public void setup() {
    id = new WritableBlockId("path", 0, 16);
    loader = new WritableBlockLoader(id, true, 4, 1, SyncMethod.WRITE_BACK);
  }

  /**
   * Test whether it is able to write data
   * and check the data locates at right position
   */
  @Test
  public void testWriteOneBlockWithOffset() throws IOException {
    byte[] toWrite = new byte[]{1,2};
    byte[] expected = new byte[]{0,0,1,2};

    loader.writeData(toWrite, 2);

    try {
      byte[] loaded = loader.getData(0);
      assertArrayEquals(loaded, expected);
    } catch (BlockLoadingException e) {
      fail();
    }

    // Index 1 is out of bounds.
    loadWithFailure(loader, 1);
  }

  /**
   * Test whether it is able to write data
   * and check the data locates at right position
   */
  @Test
  public void testWriteOneBlock() throws IOException {
    byte[] toWrite = new byte[]{1,2,3,4};
    loader.writeData(toWrite, 0);

    try {
      byte[] loaded = loader.getData(0);
      assertArrayEquals(loaded, toWrite);
    } catch (BlockLoadingException e) {
      fail();
    }

    // Index 1 is out of bounds.
    loadWithFailure(loader, 1);
  }

  /**
   * TODO should we take care of this case?
   * |12..| => |21..|
   */
  @Test
  public void testOverwrite() throws BlockLoadingException, IOException {
    loader.writeData(new byte[]{1, 2}, 0);
    loader.writeData(new byte[]{2,1}, 0);
    try {
      assertEquals(loader.getData(0)[0], 2);
    } catch (BlockLoadingException e) {
      fail();
    }
  }

  /**
   * index 0 |...4|
   * index 1 |5...|
   * index 2 null
   */
  @Test
  public void testWriteAcrossBlock() throws IOException {
    loader.writeData(new byte[]{4,5}, 3);

    loadWithSuccess(loader, 0);
    loadWithSuccess(loader, 1);
    loadWithFailure(loader, 2);
  }

  @Test
  public void testOverflow() {
    byte[] toWrite = new byte[]{1,2,3,4};
    try {
      loader.writeData(toWrite, 12);
      loader.writeData(toWrite, 16);
      fail();
    } catch (IOException e) {
      // Success
    }
  }
  /**
   * Helper function to make sure loading is done successfully.
   * @param loader
   * @param index
   */
  public void loadWithSuccess(final BlockLoader loader, final int index) {
    try {
      loader.getData(index);
    } catch (BlockLoadingException e) {
      fail();
    }
  }

  /**
   * Helper function to make sure an exception occurs while loading.
   * @param loader
   * @param index
   */
  public void loadWithFailure(final BlockLoader loader, final int index) {
    try {
      loader.getData(index);
      fail();
    } catch (BlockLoadingException e) {
      // Test success
    }
  }
}
