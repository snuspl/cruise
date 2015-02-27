package edu.snu.reef.flexion.groupcomm.interfaces;

import java.util.List;

public interface IDataGatherReceiver<T> {

    void receiveGatherData(List<T> data);

}
