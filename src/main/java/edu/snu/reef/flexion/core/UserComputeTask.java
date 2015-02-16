package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.groupcomm.interfaces.*;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

public abstract class UserComputeTask {

    public abstract void run();

    final public boolean isReduceUsed(){
       return (this instanceof IDataReduceSender);
    }

    final public boolean isGatherUsed(){
        return (this instanceof IDataGatherSender);
    }

    final public boolean isBroadcastUsed(){
        return (this instanceof IDataBroadcastReceiver);
    }

    final public boolean isScatterUsed(){
        return (this instanceof IDataScatterReceiver);
    }

    public Class<? extends Codec> getReduceCodec() {
        return SerializableCodec.class;
    }

    public Class<? extends Codec> getGatherCodec() {
        return SerializableCodec.class;
    }
}
