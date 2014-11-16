package org.apache.reef.inmemory.task.write;

import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test WritableBlockLoader.
 * Initiate the blockLoader with blockSize 131072, bufferSize 8192
 * FYI. Data in a cache block is aligned with size of {bufferSize}
 */
public class WritableBlockLoaderWriteTest {
  private BlockId id;
  private WritableBlockLoader loader;
  private static final long BLOCK_SIZE = 131072;
  private static final int BUFFER_SIZE = 8192;

  @Before
  public void setup() {
    id = new WritableBlockId("path", 0, BLOCK_SIZE);
    loader = new WritableBlockLoader(id, true, BUFFER_SIZE, 1, SyncMethod.WRITE_BACK);
  }

  /**
   * Test with tiny packets which are smaller than
   * buffer size. Fill out 1 buffer inside a block.
   */
  @Test
  public void testTinyPacket() throws IOException {
    final byte[] packet0 = generateData(BUFFER_SIZE / 2);
    final int offset0 = 0;

    final byte[] packet1 = generateData(BUFFER_SIZE / 2);
    final int offset1 = packet0.length;

    final byte[] expected = new byte[BUFFER_SIZE];
    System.arraycopy(packet0, 0, expected, offset0, packet0.length);
    System.arraycopy(packet1, 0, expected, offset1, packet1.length);

    loader.writeData(packet0, offset0);
    loader.writeData(packet1, offset1);

    try {
      byte[] loaded = loader.getData(0);
      assertArrayEquals(expected, loaded);
    } catch (BlockLoadingException e) {
      fail();
    }

    // Index 1 is out of bound for this block.
    loadWithFailure(loader, 1);
  }

  /**
   * Test to write a packet of buffer size.
   */
  @Test
  public void testPacketWithBufferSize() throws IOException {
    final byte[] packet0 = generateData(BUFFER_SIZE);
    loader.writeData(packet0, 0);

    try {
      byte[] loaded = loader.getData(0);
      assertArrayEquals(packet0, loaded);
    } catch (BlockLoadingException e) {
      fail();
    }

    // Index 1 is out of bounds.
    loadWithFailure(loader, 1);
  }

  /**
   * An exception is thrown when client tries to write
   * data to the offset written already.
   */
  @Test()
  public void testOverwrite() throws BlockLoadingException, IOException {
    final byte[] packet0 = generateData(BUFFER_SIZE);
    final byte[] packet1 = generateData(BUFFER_SIZE);

    final int offset = 0;

    loader.writeData(packet0, offset);
    try {
      loader.writeData(packet1, offset);
      fail();
    } catch (IOException e) {
      // Success
    }
  }

  /**
   * Test for the case when the packets are not aligned
   * to the size of buffer. (e.g, if the packets split
   * 3 buffers to 4 splits.)
   */
  @Test
  public void testWriteAcrossBuffer() throws IOException, BlockLoadingException {
    final int numBuffers = 3;
    final int numSplits = 4;

    final byte[] data = generateData(numBuffers * BUFFER_SIZE);

    final int packetLength = numBuffers * BUFFER_SIZE / numSplits;

    // Fill out buffers
    final byte[][] packets = new byte[numSplits][packetLength];
    for (int packetIndex = 0; packetIndex < packets.length; packetIndex++) {
      System.arraycopy(data, packetIndex * packetLength, packets[packetIndex], 0, packetLength);
      loader.writeData(packets[packetIndex], packetIndex * packetLength);
    }

    // Collect the loaded buffers and compare to the original data.
    ByteBuffer loaded = ByteBuffer.allocate(data.length);
    for (int bufferIndex = 0; bufferIndex < numBuffers; bufferIndex++) {
      loaded.put(loader.getData(bufferIndex));
    }
    assertArrayEquals(data, loaded.array());

    // The data should be written as amount of {numBuffers}
    loadWithFailure(loader, numBuffers);
  }

  /**
   * Test to fill one block with packets.
   * @throws IOException
   * @throws BlockLoadingException
   */
  @Test
  public void testFillOneBlock() throws IOException, BlockLoadingException {
    final byte[] data = generateData((int) BLOCK_SIZE);

    // Fill out the block
    for (int offset = 0; offset < BLOCK_SIZE; offset += BUFFER_SIZE) {
      byte[] packet = new byte[BUFFER_SIZE];

      System.arraycopy(data, offset, packet, 0, BUFFER_SIZE);
      loader.writeData(packet, offset);
    }

    // Collect the loaded buffers and compare to the original data.
    ByteBuffer loaded = ByteBuffer.allocate(data.length);
    for (int bufferIndex = 0; bufferIndex < BLOCK_SIZE / BUFFER_SIZE; bufferIndex++) {
      loaded.put(loader.getData(bufferIndex));
    }
    assertArrayEquals(data, loaded.array());
  }

  /**
   * Test to insert data over the capacity of a block.
   */
  @Test
  public void testOverflow() throws IOException {
    final int length0 = new Random().nextInt((int) BLOCK_SIZE);
    final int length1 = (int)BLOCK_SIZE - length0 + 1;

    // Fill out one block
    byte[] packet0 = generateData(length0);
    loader.writeData(packet0, 0);

    // Write another packet
    byte[] packet1 = generateData(length1);

    try {
      loader.writeData(packet1, length0);
      fail();
    } catch (IOException e) {
      // Success
    }
  }

  /**
   * Helper method to make sure an exception occurs while loading.
   * @param loader
   * @param index
   */
  private void loadWithFailure(final BlockLoader loader, final int index) {
    try {
      loader.getData(index);
      fail();
    } catch (BlockLoadingException e) {
      // Test success
    }
  }

  /**
   * Helper method to generate a random byte array.
   * @param size
   * @return
   */
  private byte[] generateData(final int size) {
    final byte[] result = new byte[size];
    new Random().nextBytes(result);
    return result;
  }
}
