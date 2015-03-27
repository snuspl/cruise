package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataScatterReceiver;

public abstract class UserComputeTask <T> {

    public abstract void run(int iteration, T data);

    public void initialize(KeyValueStore keyValueStore){

    }

    public void cleanup(KeyValueStore keyValueStore) {

    }

    final public boolean isReduceUsed() {
       return (this instanceof DataReduceSender);
    }

    final public boolean isGatherUsed() {
        return (this instanceof DataGatherSender);
    }

    final public boolean isBroadcastUsed() {
        return (this instanceof DataBroadcastReceiver);
    }

    final public boolean isScatterUsed() {
        return (this instanceof DataScatterReceiver);
    }
}
