/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.ps.common.resolver;

import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that StaticServerResolver creates a balanced assignment of partitions to physical servers.
 */
public final class StaticServerResolverTest {
  private StaticServerResolver newStaticServerResolver(final int numServers,
                                                       final int numPartitions) throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumServers.class, Integer.toString(numServers))
        .bindNamedParameter(NumPartitions.class, Integer.toString(numPartitions))
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    return injector.getInstance(StaticServerResolver.class);
  }

  /**
   * Test the mapping with a single server and ten partitions.
   */
  @Test
  public void testSingleServer() throws InjectionException {
    final int numServers = 1;
    final int numPartitions = 10;
    final ServerResolver resolver = newStaticServerResolver(numServers, numPartitions);

    final List<Integer> partitions = resolver.getPartitions(SERVER_ID_PREFIX + 0);
    assertEquals(numPartitions, partitions.size());
    for (int i = 0; i < numPartitions; i++) {
      assertTrue(partitions.contains(i));
    }

    // Check resolution for first 100 hashes
    for (int i = 0; i < 100; i++) {
      assertEquals(SERVER_ID_PREFIX + 0, resolver.resolveServer(i));
    }
  }

  /**
   * Test the mapping with three servers and twenty partitions.
   */
  @Test
  public void testThreeServers() throws InjectionException {
    final int numServers = 3;
    final int numPartitions = 20;
    final ServerResolver resolver = newStaticServerResolver(numServers, numPartitions);

    final Set<Integer> allPartitions = new HashSet<>(numPartitions);

    for (int i = 0; i < numServers; i++) {
      final List<Integer> partitions = resolver.getPartitions(SERVER_ID_PREFIX + i);
      assertTrue(partitions.size() == numPartitions / numServers ||
          partitions.size() == numPartitions / numServers + 1);
      allPartitions.addAll(partitions);
    }
    assertEquals(numPartitions, allPartitions.size());
    for (int i = 0; i < numPartitions; i++) {
      assertTrue(allPartitions.contains(i));
    }

    // Check resolution for first 100 hashes maps to a server
    for (int i = 0; i < 100; i++) {
      assertTrue(resolver.resolveServer(i).startsWith(SERVER_ID_PREFIX));
    }
  }
}
