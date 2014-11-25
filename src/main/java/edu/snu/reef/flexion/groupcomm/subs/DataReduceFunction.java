package edu.snu.reef.flexion.groupcomm.subs;

import com.microsoft.reef.io.network.group.operators.Reduce;

import java.util.ArrayList;
import java.util.List;

public final class DataReduceFunction implements Reduce.ReduceFunction<List<Integer>> {

  @Override
  public final List<Integer> apply(Iterable<List<Integer>> lists) {
    List<Integer> retList = new ArrayList<>();
    for (final List<Integer> list : lists) {
      retList.addAll(list);
    }

    return retList;
  }
}
