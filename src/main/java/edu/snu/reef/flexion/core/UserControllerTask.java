package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataScatterSender;

/**
 * Abstract class for user-defined controller tasks.
 * This class should be extended by user-defined controller tasks that override run, initialize, and cleanup methods
 */
public abstract class UserControllerTask {

    /**
     * Main process of a user-defined controller task
     * @param iteration
     */
    public abstract void run(int iteration);

    /**
     * Initialize a user-defined controller task.
     * Default behavior of this method is to do nothing, but this method can be overridden in subclasses
     */
    public void initialize() {
        return;
    }

    /**
     * Clean up a user-defined controller task
     * Default behavior of this method is to do nothing, but this method can be overridden in subclasses
     */
    public void cleanup() {
        return;
    }

    public abstract boolean isTerminated(int iteration);

    final public boolean isReduceUsed() {
        return (this instanceof DataReduceReceiver);
    }

    final public boolean isGatherUsed() {
        return (this instanceof DataGatherReceiver);
    }

    final public boolean isBroadcastUsed() {
        return (this instanceof DataBroadcastSender);
    }

    final public boolean isScatterUsed() {
        return (this instanceof DataScatterSender);
    }
}
