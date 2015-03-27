package edu.snu.reef.flexion.groupcomm.interfaces;

import java.util.List;

public interface DataGatherReceiver<T> {

    void receiveGatherData(List<T> data);

}
