package org.apache.reef.inmemory.task.hdfs;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;
import org.apache.reef.inmemory.task.BlockReceiver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Test HdfsBlockReceiver.
 * Initiate the blockLoader with blockSize 131072, bufferSize 8192
 */
public class HdfsBlockReceiverTest {
  private BlockReceiver receiver;
  private static final long BLOCK_SIZE = 131072;
  private static final int BUFFER_SIZE = 8192;

  @Before
  public void setup() {
    final BlockId id = new BlockId("path", 0);
    receiver = new HdfsBlockReceiver(id, BLOCK_SIZE, false, BUFFER_SIZE);
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

    receiver.writeData(packet0, offset0);
    receiver.writeData(packet1, offset1);
    receiver.completeWrite();

    assertLoadSuccess(expected, receiver);

    // Index 1 is out of bound for this block.
    assertLoadFailsWithException(receiver, 1);
  }

  /**
   * Test to write a packet of buffer size.
   */
  @Test
  public void testPacketWithBufferSize() throws IOException {
    final byte[] packet = generateData(BUFFER_SIZE);
    receiver.writeData(packet, 0);
    receiver.completeWrite();

    assertLoadSuccess(packet, receiver);

    // Index 1 is out of bound.
    assertLoadFailsWithException(receiver, 1);
  }

  /**
   * An exception is thrown when client tries to write
   * data to the offset written already.
   */
  @Test
  public void testOverwrite() throws BlockLoadingException, IOException {
    final byte[] packet0 = generateData(BUFFER_SIZE);
    final byte[] packet1 = generateData(BUFFER_SIZE);

    final int offset = 0;

    receiver.writeData(packet0, offset);

    // Write fails because it tries to write with the same offset.
    assertWriteFailsWithException(receiver, packet1, offset);
  }

  /**
   * Test for the case when the packets are not aligned
   * to the size of buffer. (e.g, if the packets split
   * 3 buffers to 4 splits.)
   */
  @Test
  public void testWriteAcrossBuffer() throws IOException, BlockWritingException {
    final int numBuffers = 3;
    final int numSplits = 4;

    final byte[] data = generateData(numBuffers * BUFFER_SIZE);

    final int packetLength = numBuffers * BUFFER_SIZE / numSplits;

    // Fill out buffers
    final byte[][] packets = new byte[numSplits][packetLength];
    for (int packetIndex = 0; packetIndex < packets.length; packetIndex++) {
      System.arraycopy(data, packetIndex * packetLength, packets[packetIndex], 0, packetLength);
      receiver.writeData(packets[packetIndex], packetIndex * packetLength);
    }
    receiver.completeWrite();

    // Collect the loaded buffers and compare to the original data.
    final ByteBuffer loaded = ByteBuffer.allocate(data.length);
    for (int bufferIndex = 0; bufferIndex < numBuffers; bufferIndex++) {
      loaded.put(receiver.getData(bufferIndex));
    }
    assertArrayEquals(data, loaded.array());

    // The data should be written as amount of {numBuffers}
    assertLoadFailsWithException(receiver, numBuffers);
  }

  /**
   * Test to fill one block with packets.
   */
  @Test
  public void testFillOneBlock() throws IOException, BlockWritingException {
    final byte[] data = generateData((int) BLOCK_SIZE);

    // Fill out the block
    for (int offset = 0; offset < BLOCK_SIZE; offset += BUFFER_SIZE) {
      final byte[] packet = new byte[BUFFER_SIZE];

      System.arraycopy(data, offset, packet, 0, BUFFER_SIZE);
      receiver.writeData(packet, offset);
    }
    receiver.completeWrite();

    // Collect the loaded buffers and compare to the original data.
    final ByteBuffer loaded = ByteBuffer.allocate(data.length);
    for (int bufferIndex = 0; bufferIndex < BLOCK_SIZE / BUFFER_SIZE; bufferIndex++) {
      loaded.put(receiver.getData(bufferIndex));
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

    // Write the first packet
    final byte[] packet0 = generateData(length0);
    receiver.writeData(packet0, 0);
    receiver.completeWrite();

    // Write the other packet, then write fails because the packet exceeds the block size.
    final byte[] packet1 = generateData(length1);
    assertWriteFailsWithException(receiver, packet1, length0);
  }

  /**
   * TODO Change LoadSuccess -> WriteSuccess
   * Helper method to make sure load succeed without exception.
   */
  private void assertLoadSuccess(final byte[] expected, final BlockReceiver receiver) {
    try {
      final byte[] loaded = receiver.getData(0);
      assertArrayEquals(expected, loaded);
    } catch (BlockWritingException e) {
      fail();
    }
  }

  /**
   * TODO fix the comment
   * Helper method to make sure an exception occurs while loading.
   */
  private void assertLoadFailsWithException(final BlockReceiver receiver, final int index) {
    try {
      receiver.getData(index);
      fail();
    } catch (BlockWritingException e) {
      // Test success
    }
  }

  /**
   * TODO Improve exception handling
   * Helper method to make sure an exception occurs while writing.
   */
  private void assertWriteFailsWithException(final BlockReceiver receiver, final byte[] packet, final long offset) {
    try {
      receiver.writeData(packet, offset);
      fail();
    } catch (IOException e) {
      // Test success
    }
  }

  /**
   * Helper method to generate a random byte array.
   */
  private byte[] generateData(final int size) {
    final byte[] result = new byte[size];
    new Random().nextBytes(result);
    return result;
  }
}
