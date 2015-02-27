package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataGatherSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataScatterReceiver;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

public abstract class UserComputeTask <T> {

    public abstract void run(T data);

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

    public abstract Class<? extends DataParser<T>> getDataParserClass();

    /**
     * Return the default codec
     * To use another codec, override this function
     */
    public Class<? extends Codec> getReduceCodecClass() {
        return SerializableCodec.class;
    }

    /**
     * Return the default codec
     * To use another codec, override this function
     */
    public Class<? extends Codec> getGatherCodecClass() {
        return SerializableCodec.class;
    }
}
