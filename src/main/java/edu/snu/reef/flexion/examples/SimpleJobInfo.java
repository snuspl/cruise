package edu.snu.reef.flexion.examples;


import edu.snu.reef.flexion.core.*;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class SimpleJobInfo implements UserJobInfo{

    @Inject
    public SimpleJobInfo(){
    }

    @Override
    public List<UserTaskInfo> getTaskInfoList() {
        List<UserTaskInfo> taskInfoList = new LinkedList<>();
        taskInfoList.add(new UserTaskInfo(SimpleCmpTask.class, SimpleCtrlTask.class, SimpleCommGroup.class)
                .setBroadcast(SerializableCodec.class)
                .setReduce(SerializableCodec.class, SimpleReduceFunction.class));
        return taskInfoList;
    }

    @Override
    public Class<? extends DataParser> getDataParser() {
        return SimpleDataParser.class;
    }
}
