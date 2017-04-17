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
package edu.snu.cay.dolphin.async.tensorflow.sample_example;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.tensorflow.TensorflowParameters;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;
import org.bytedeco.javacpp.tensorflow;

import javax.inject.Inject;
import java.nio.FloatBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.bytedeco.javacpp.tensorflow.*;

final class TensorflowTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(TensorflowTrainer.class.getName());

  private final boolean useGpu;
  private final float gpuMemoryFraction;
  private final int miniBatchSize;
  private final ParameterWorker parameterWorker;
  private final VectorFactory vectorFactory;
  private tensorflow.Session session;
  private tensorflow.GraphDef graphDef;

  @Inject
  private TensorflowTrainer(@Parameter(TensorflowParameters.UseGpu.class) final boolean useGpu,
                            @Parameter(TensorflowParameters.GpuMemoryFraction.class) final float gpuMemoryFraction,
                            @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                            final ParameterWorker<Integer, Vector, Vector> parameterWorker,
                            final VectorFactory vectorFactory) {
    this.useGpu = useGpu;
    this.gpuMemoryFraction = gpuMemoryFraction;
    this.miniBatchSize = miniBatchSize;
    this.parameterWorker = parameterWorker;
    this.vectorFactory = vectorFactory;
  }

  @Override
  public void initialize() {
    final Scope root = Scope.NewRootScope();

    // a = [3 2; -1 0]
    final Output a = Const(root, Tensor.create(new float[] {3.f, 2.f, -1.f, 0.f}, new TensorShape(2, 2)));

    // x = [1.0; 1.0]
    final Output x = Const(root.WithOpName("x"), Tensor.create(new float[] {1.f, 1.f}, new TensorShape(2, 1)));

    // y = a * x
    final MatMul y = new MatMul(root.WithOpName("y"), new Input(a), new Input(x));

    // y2 = y.^2
    final Square y2 = new Square(root, y.asInput());

    // y2Sum = sum(y2)
    final Sum y2Sum = new Sum(root, y2.asInput(), new Input(0));

    // yNorm = sqrt(y2Sum)
    final Sqrt yNorm = new Sqrt(root, y2Sum.asInput());

    // y_normalized = y ./ yNorm
    new Div(root.WithOpName("y_normalized"), y.asInput(), yNorm.asInput());

    this.graphDef = new GraphDef();
    Status s = root.ToGraphDef(graphDef);
    if (!s.ok()) {
      throw new RuntimeException(s.error_message().getString());
    }


    final SessionOptions options = new SessionOptions();
    if (useGpu) {
      options.config().gpu_options().set_per_process_gpu_memory_fraction(gpuMemoryFraction);
    }
    this.session = new Session(options);
    if (options.target() == null) {
      SetDefaultDevice(useGpu ? "/gpu:0" : "/cpu:0", graphDef);
    }

    s = session.Create(graphDef);
    if (!s.ok()) {
      throw new RuntimeException(s.error_message().getString());
    }

    // Randomly initialize the input.
    final double[] parameter = new double[2];
    parameter[0] = (float) Math.random();
    parameter[1] = (float) Math.random();
    final float invNorm = 1 / (float) Math.sqrt(parameter[0] * parameter[0] + parameter[1] * parameter[1]);
    parameter[0] = parameter[0] * invNorm;
    parameter[1] = parameter[1] * invNorm;

    final Vector parameterVec = vectorFactory.createDense(parameter);
    parameterWorker.push(0, parameterVec);

  }

  @Override
  public void run(final int iteration) {
    for (int step = 0; step < miniBatchSize; step++) {
      final Vector parameterVec = (Vector) parameterWorker.pull(0);
      final tensorflow.Tensor x = new tensorflow.Tensor(DT_FLOAT, new tensorflow.TensorShape(2, 1));
      final FloatBuffer xFlat = x.createBuffer();
      xFlat.put(0, (float) parameterVec.get(0));
      xFlat.put(1, (float) parameterVec.get(1));
      LOG.log(Level.INFO, "Pulled parameter : {0}, {1}", new Object[]{parameterVec.get(0), parameterVec.get(1)});

      final tensorflow.TensorVector outputs = new tensorflow.TensorVector();
      outputs.resize(0);
      final Status s = session.Run(new tensorflow.StringTensorPairVector(new String[]{"x"}, new tensorflow.Tensor[]{x}),
          new tensorflow.StringVector("y_normalized:0"), new tensorflow.StringVector(), outputs);
      if (!s.ok()) {
        throw new RuntimeException(s.error_message().getString());
      }
      assert outputs.size() == 1;

      final tensorflow.Tensor yNorm = outputs.get(0);

      final double[] newParameter = new double[2];
      newParameter[0] = yNorm.tensor_data().asBuffer().asFloatBuffer().get(0);
      newParameter[1] = yNorm.tensor_data().asBuffer().asFloatBuffer().get(1);
      parameterWorker.push(0, vectorFactory.createDense(newParameter));
      LOG.log(Level.INFO, "Pushed parameter : {0}, {1}", new Object[]{newParameter[0], newParameter[1]});
    }
  }

  @Override
  public void cleanup() {
    final tensorflow.Status s = session.Close();
    if (!s.ok()) {
      throw new RuntimeException(s.error_message().getString());
    }
  }
}
