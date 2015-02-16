package edu.snu.reef.flexion.groupcomm.interfaces;

import org.apache.reef.io.serialization.Codec;

public interface IDataBroadcastSender<T> {

    Class<? extends Codec> getBroadcastCodec();

    T sendBroadcastData();

}
