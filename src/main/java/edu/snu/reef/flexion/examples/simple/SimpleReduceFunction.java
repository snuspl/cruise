package edu.snu.reef.flexion.examples.simple;

import com.microsoft.reef.io.network.group.operators.Reduce;
import javax.inject.Inject;

public class SimpleReduceFunction implements Reduce.ReduceFunction<Integer> {

    @Inject
    public SimpleReduceFunction() {
    }

    @Override
    public final Integer apply(Iterable<Integer> dataList) {
        Integer sum = 0;
        Integer count = 0;
        for (final Integer data : dataList) {
            sum += data;
            count++;
        }

        return sum/count;
    }
}