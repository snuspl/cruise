package edu.snu.reef.flexion.groupcomm.interfaces;

import org.apache.reef.io.serialization.Codec;

import java.util.List;

public interface IDataScatterSender<T> {

    Class<? extends Codec> getScatterCodec();

    List<T> sendScatterData();

}
