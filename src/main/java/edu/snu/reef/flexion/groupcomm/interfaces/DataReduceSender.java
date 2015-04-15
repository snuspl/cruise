package edu.snu.reef.flexion.groupcomm.interfaces;

public interface DataReduceSender<T> {
  T sendReduceData(int iteration);
}
