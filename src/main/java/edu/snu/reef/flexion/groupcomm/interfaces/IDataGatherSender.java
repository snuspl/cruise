package edu.snu.reef.flexion.groupcomm.interfaces;

import org.apache.reef.io.serialization.Codec;

public interface IDataGatherSender<T> {

    Class<? extends Codec> getGatherCodecClass();

    T sendGatherData();

}
