package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataScatterSender;

public abstract class UserControllerTask {

    public abstract void run(int iteration);

    public void initialize(KeyValueStore keyValueStore) {

    }

    public void cleanup(KeyValueStore keyValueStore) {

    }

    public abstract boolean isTerminated(int iteration);

    final public boolean isReduceUsed(){
        return (this instanceof DataReduceReceiver);
    }

    final public boolean isGatherUsed(){
        return (this instanceof DataGatherReceiver);
    }

    final public boolean isBroadcastUsed(){
        return (this instanceof DataBroadcastSender);
    }

    final public boolean isScatterUsed(){
        return (this instanceof DataScatterSender);
    }
}
