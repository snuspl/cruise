package edu.snu.cay.services.em;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TestUtils {

  /**
   * Use a thread pool to concurrently execute threads.
   * Note that this method does NOT wait for the termination of all threads before returning.
   */
  public static void runConcurrently(final Runnable[] threads) throws InterruptedException {
    final ExecutorService pool = Executors.newFixedThreadPool(threads.length);
    for (final Runnable thread : threads) {
      pool.submit(thread);
    }
    pool.shutdown();
  }
}
