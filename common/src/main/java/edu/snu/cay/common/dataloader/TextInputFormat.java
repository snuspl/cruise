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
package edu.snu.cay.common.dataloader;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;

import com.google.common.base.Charsets;
import org.apache.hadoop.mapred.*;

/**
 * An {@link InputFormat} for plain text files. Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.
 * Keys are the position in the file, and values are the line of text.
 *
 * It's a resembled from {@link org.apache.hadoop.mapred.TextInputFormat} and modified to
 * extend {@link ExactNumSplitFileInputFormat} to break the dependency between
 * the number of HDFS blocks and the number of {@link InputSplit}s.
 */
public class TextInputFormat extends ExactNumSplitFileInputFormat<LongWritable, Text>
    implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;

  public void configure(final JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }

  protected boolean isSplitable(final FileSystem fs, final Path file) {
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    return null == codec || codec instanceof SplittableCompressionCodec;
  }

  public RecordReader<LongWritable, Text> getRecordReader(
      final InputSplit genericSplit, final JobConf job,
      final Reporter reporter)
      throws IOException {

    reporter.setStatus(genericSplit.toString());
    final String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }
    return new LineRecordReader(job, (FileSplit) genericSplit,
        recordDelimiterBytes);
  }
}
