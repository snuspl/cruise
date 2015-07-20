package edu.snu.cay.services.em.evaluator.impl;

import org.junit.Before;
import org.junit.Test;

/**
 * Run the tests of MemoryStoreTestSuite with LocalMemoryStore.
 */
public final class LocalMemoryStoreTest {

  private LocalMemoryStore localMemoryStore;

  @Before
  public void setUp() {
    localMemoryStore = new LocalMemoryStore();
  }

  @Test
  public void testMultiThreadAdd() throws InterruptedException {
    MemoryStoreTestSuite.multithreadAdd(localMemoryStore);
  }

  @Test
  public void testMultiThreadAddList() throws InterruptedException {
    MemoryStoreTestSuite.multithreadAddList(localMemoryStore);
  }

  @Test
  public void testMultiThreadRemove() throws InterruptedException {
    MemoryStoreTestSuite.multithreadRemove(localMemoryStore);
  }

  @Test
  public void testMultiThreadRemoveList() throws InterruptedException {
    MemoryStoreTestSuite.multithreadRemoveList(localMemoryStore);
  }

  @Test
  public void testMultiThreadAddGetRemoveList() throws InterruptedException {
    MemoryStoreTestSuite.multithreadAddRemoveGetIterateList(localMemoryStore);
  }
}
