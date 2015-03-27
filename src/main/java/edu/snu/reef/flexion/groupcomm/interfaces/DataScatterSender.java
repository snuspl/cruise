package edu.snu.reef.flexion.groupcomm.interfaces;

import org.apache.reef.io.serialization.Codec;

import java.util.List;

public interface DataScatterSender<T> {

    List<T> sendScatterData(int iteration);

}
