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
package edu.snu.cay.utils;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test for the {@link DAGImpl} class.
 * It is based on the one of MIST.
 */
public final class DAGImplTest {

  @Test
  public void addVertexTest() {
    final List<Integer> expected = ImmutableList.of(1, 2, 3, 4);
    final DAG<Integer> dag = new DAGImpl<>();
    dag.addVertex(1);
    dag.addVertex(2);
    dag.addVertex(3);
    dag.addVertex(4);
    assertEquals(new HashSet<>(expected), dag.getRootVertices());
  }

  @Test
  public void removeVertexTest() {
    final List<Integer> expected = ImmutableList.of(1, 3);
    final DAG<Integer> dag = new DAGImpl<>();
    dag.addVertex(1);
    dag.addVertex(2);
    dag.addVertex(3);
    dag.addVertex(4);
    dag.removeVertex(2);
    dag.removeVertex(4);
    assertEquals(new HashSet<>(expected), dag.getRootVertices());
  }

  @Test
  public void addAndRemoveEdgeTest() {
    final DAG<Integer> dag = new DAGImpl<>();
    dag.addVertex(1);
    dag.addVertex(2);
    dag.addVertex(3);
    dag.addVertex(4);
    dag.addEdge(1, 3);
    dag.addEdge(3, 4);
    dag.addEdge(2, 4);

    assertTrue(dag.isAdjacent(1, 3));
    assertTrue(dag.isAdjacent(3, 4));
    assertTrue(dag.isAdjacent(2, 4));
    assertFalse(dag.isAdjacent(1, 2));
    assertFalse(dag.isAdjacent(2, 3));
    assertFalse(dag.isAdjacent(1, 4));

    // check root vertices
    final List<Integer> expectedRoot = ImmutableList.of(1, 2);
    assertEquals("Root vertices should be " + expectedRoot,
        new HashSet<>(expectedRoot), dag.getRootVertices());

    final Set<Integer> n = dag.getNeighbors(1);
    assertEquals(new HashSet<>(ImmutableList.of(3)), n);
    assertEquals(2, dag.getInDegree(4));
    assertEquals(1, dag.getInDegree(3));
    assertEquals(0, dag.getInDegree(1));

    dag.removeEdge(1, 3);
    assertFalse(dag.isAdjacent(1, 3));
    assertEquals(dag.getInDegree(3), 0);
    // check root vertices
    final List<Integer> expectedRoot2 = ImmutableList.of(1, 2, 3);
    assertEquals("Root vertices should be " + expectedRoot2,
        new HashSet<>(expectedRoot2), dag.getRootVertices());

    dag.removeEdge(3, 4);
    assertFalse(dag.isAdjacent(3, 4));
    assertEquals(dag.getInDegree(4), 1);
    // check root vertices
    final List<Integer> expectedRoot3 = ImmutableList.of(1, 2, 3);
    assertEquals("Root vertices should be " + expectedRoot3,
        new HashSet<>(expectedRoot3), dag.getRootVertices());

    dag.removeEdge(2, 4);
    assertFalse(dag.isAdjacent(2, 4));
    assertEquals(dag.getInDegree(4), 0);
    // check root vertices
    final List<Integer> expectedRoot4 = ImmutableList.of(1, 2, 3, 4);
    assertEquals("Root vertices should be " + expectedRoot4,
        new HashSet<>(expectedRoot4), dag.getRootVertices());
  }
}
