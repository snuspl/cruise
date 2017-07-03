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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.dolphin.async.ETDolphinConfiguration;
import edu.snu.cay.dolphin.async.jobserver.JobLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.mlapps.lasso.LassoParameters.*;

/**
 * Application launching code for Lasso with JobServer.
 */
public final class LassoJob {

  @Inject
  private LassoJob() {
  }

  public static void main(final String[] args) {
    JobLauncher.submitJob("Lasso", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(LassoTrainer.class)
        .setInputParserClass(LassoETParser.class)
        .setInputKeyCodecClass(SerializableCodec.class)
        .setInputValueCodecClass(LassoDataCodec.class)
        .setModelUpdateFunctionClass(LassoETModelUpdateFunction.class)
        .setModelKeyCodecClass(SerializableCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(NumFeatures.class)
        .addParameterClass(StepSize.class)
        .addParameterClass(Lambda.class)
        .addParameterClass(ModelGaussian.class)
        .addParameterClass(DecayRate.class)
        .addParameterClass(DecayPeriod.class)
        .addParameterClass(NumFeaturesPerPartition.class)
        .build());
  }
}
