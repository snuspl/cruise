package edu.snu.cay.services.em.evaluator.impl;

import org.apache.htrace.SpanReceiver;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Run the tests of MemoryStoreTestSuite with ElasticMemoryStore.
 */
public final class ElasticMemoryStoreTest {

  private ElasticMemoryStore elasticMemoryStore;

  @Before
  public void setUp() {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      injector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));
      elasticMemoryStore =  injector.getInstance(ElasticMemoryStore.class);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException("InjectionException while injecting ElasticMemoryStore", e);
    }
  }

  @Test
  public void testMultiThreadAdd() throws InterruptedException {
    MemoryStoreTestSuite.multithreadAdd(elasticMemoryStore);
  }

  @Test
  public void testMultiThreadAddList() throws InterruptedException {
    MemoryStoreTestSuite.multithreadAddList(elasticMemoryStore);
  }

  @Test
  public void testMultiThreadRemove() throws InterruptedException {
    MemoryStoreTestSuite.multithreadRemove(elasticMemoryStore);
  }

  @Test
  public void testMultiThreadRemoveList() throws InterruptedException {
    MemoryStoreTestSuite.multithreadRemoveList(elasticMemoryStore);
  }

  @Test
  public void testMultiThreadAddGetRemoveList() throws InterruptedException {
    MemoryStoreTestSuite.multithreadAddRemoveGetIterateList(elasticMemoryStore);
  }
}
