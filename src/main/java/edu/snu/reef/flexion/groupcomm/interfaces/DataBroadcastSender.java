package edu.snu.reef.flexion.groupcomm.interfaces;

public interface DataBroadcastSender<T> {

    T sendBroadcastData(int iteration);

}
