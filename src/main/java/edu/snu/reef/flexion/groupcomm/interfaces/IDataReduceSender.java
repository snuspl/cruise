package edu.snu.reef.flexion.groupcomm.interfaces;

import com.microsoft.reef.io.network.group.operators.Reduce;
import org.apache.reef.io.serialization.Codec;

public interface IDataReduceSender<T> {

    Class<? extends Codec> getReduceCodecClass();

    T sendReduceData(int iteration);

    Class<? extends Reduce.ReduceFunction<T>> getReduceFunctionClass();

}
