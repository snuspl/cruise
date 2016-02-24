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
package edu.snu.cay.async.examples.lda;

import edu.snu.cay.async.Worker;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
final class LdaWorker implements Worker {

  private final LdaDataParser dataParser;
  private final ParameterWorker<Integer, int[], int[]> parameterWorker;
  private final SparseLdaSampler sampler;
  private final int numVocabs;
  private List<Document> documents;

  @Inject
  private LdaWorker(final LdaDataParser dataParser,
                    final ParameterWorker<Integer, int[], int[]> parameterWorker,
                    final SparseLdaSampler sampler,
                    @Parameter(LdaREEF.NumVocabs.class) final int numVocabs) {
    this.dataParser = dataParser;
    this.parameterWorker = parameterWorker;
    this.sampler = sampler;
    this.numVocabs = numVocabs;
  }

  @Override
  public void initialize() {
    this.documents = dataParser.parse();

    for (final Document document : documents) {

      for (int i = 0; i < document.size(); i++) {
        final int word = document.getWord(i);
        parameterWorker.push(word, new int[]{document.getAssignment(i), 1});
        parameterWorker.push(numVocabs, new int[]{document.getAssignment(i), 1});
      }
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    int x = 0;
    for (final Document document : documents) {
      sampler.sample(document);
      System.out.println(++x + " documents are sampled");
    }
  }

  @Override
  public void cleanup() {
  }
}
