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
package edu.snu.cay.common.dataloader;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.*;

/**
 * A serializer class that serializes {@link HdfsSplitInfo}s into String
 * using the below {@link HdfsSplitInfoCodec} that encodes and decodes {@link HdfsSplitInfo}s.
 */
public final class HdfsSplitInfoSerializer {
  private static final HdfsSplitInfoCodec CODEC = new HdfsSplitInfoCodec();

  // utility class should not be instantiated
  private HdfsSplitInfoSerializer() {
  }

  public static String serialize(final HdfsSplitInfo hdfsSplitInfo) {
    return Base64.encodeBase64String(CODEC.encode(hdfsSplitInfo));
  }

  public static HdfsSplitInfo deserialize(final String serializedHdfsSplitInfo) {
    return CODEC.decode(Base64.decodeBase64(serializedHdfsSplitInfo));
  }

  /**
   * A codec class for {@link HdfsSplitInfo}.
   */
  public static class HdfsSplitInfoCodec implements Codec<HdfsSplitInfo> {

    @Override
    public byte[] encode(final HdfsSplitInfo hdfsSplitInfo) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream daos = new DataOutputStream(baos)) {
        daos.writeUTF(hdfsSplitInfo.getInputPath());
        daos.writeUTF(hdfsSplitInfo.getInputFormatClassName());
        hdfsSplitInfo.getInputSplit().write(daos);
        return baos.toByteArray();
      } catch (final IOException e) {
        throw new RuntimeException("Could not serialize JobConf", e);
      }
    }

    @Override
    public HdfsSplitInfo decode(final byte[] bytes) {
      final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      try (DataInputStream dais = new DataInputStream(bais)) {
        final String inputPath = dais.readUTF();
        final String inputFormatClassName = dais.readUTF();

        final JobConf jobConf;
        try {
          final Tang tang = Tang.Factory.getTang();
          jobConf = tang.newInjector(
              tang.newConfigurationBuilder()
                  .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClassName)
                  .bindNamedParameter(JobConfExternalConstructor.InputPath.class, inputPath)
                  .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
                  .build())
              .getInstance(JobConf.class);
        } catch (final InjectionException e) {
          throw new RuntimeException("Exception while injecting JobConf", e);
        }

        final FileSplit inputSplit = ReflectionUtils.newInstance(FileSplit.class, jobConf);
        inputSplit.readFields(dais);

        return HdfsSplitInfo.newBuilder()
            .setInputPath(inputPath)
            .setInputSplit(inputSplit)
            .setInputFormatClassName(inputFormatClassName)
            .build();
      } catch (final IOException e) {
        throw new RuntimeException("Could not de-serialize JobConf", e);
      }
    }
  }
}
