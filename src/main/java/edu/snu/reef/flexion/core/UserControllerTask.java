package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataGatherReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataScatterSender;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

public abstract class UserControllerTask {

    public abstract void run();

    public abstract boolean isTerminated();

    final public boolean isReduceUsed(){
        return (this instanceof IDataReduceReceiver);
    }

    final public boolean isGatherUsed(){
        return (this instanceof IDataGatherReceiver);
    }

    final public boolean isBroadcastUsed(){
        return (this instanceof IDataBroadcastSender);
    }

    final public boolean isScatterUsed(){
        return (this instanceof IDataScatterSender);
    }

    /**
     * Return the default codec
     * To use another codec, override this function
     */
    public Class<? extends Codec> getBroadcastCodec() {
        return SerializableCodec.class;
    }

    /**
     * Return the default codec
     * To use another codec, override this function
     */
    public Class<? extends Codec> getScatterCodec() {
        return SerializableCodec.class;
    }
}
