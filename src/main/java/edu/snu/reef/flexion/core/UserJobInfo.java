package edu.snu.reef.flexion.core;

import java.util.List;

public interface UserJobInfo {

    public abstract List<UserTaskInfo> getTaskInfoList();

    public abstract Class<? extends DataParser> getDataParser();

}
