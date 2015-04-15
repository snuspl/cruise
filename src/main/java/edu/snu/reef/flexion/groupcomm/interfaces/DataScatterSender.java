package edu.snu.reef.flexion.groupcomm.interfaces;

import java.util.List;

public interface DataScatterSender<T> {
  List<T> sendScatterData(int iteration);
}
