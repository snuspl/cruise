/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.pregel.graphapps.shortestpath;

import edu.snu.cay.pregel.PregelConfiguration;
import edu.snu.cay.pregel.PregelLauncher;
import edu.snu.cay.pregel.common.DefaultEdgeCodec;
import edu.snu.cay.pregel.common.DefaultGraphParser;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * Application launching code for Shortest path.
 * It calculates all vertices distances from given {@link SourceId}.
 */
public final class ShortestPathET {

  /**
   * Should not be initialized this.
   */
  private ShortestPathET() {

  }

  public static void main(final String[] args) throws IOException, InjectionException {
    PregelLauncher.launch(args, PregelConfiguration.newBuilder()
        .setComputationClass(ShortestPathComputation.class)
        .setDataParserClass(DefaultGraphParser.class)
        .setMessageValueCodecClass(StreamingSerializableCodec.class)
        .setVertexValueCodecClass(StreamingSerializableCodec.class)
        .setEdgeCodecClass(DefaultEdgeCodec.class)
        .addParameterClass(SourceId.class)
        .build());
  }
}
