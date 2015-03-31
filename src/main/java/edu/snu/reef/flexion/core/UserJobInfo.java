package edu.snu.reef.flexion.core;

import java.util.List;

/**
 * Interface for a user-defined job, which is a unit of work in Flexion.
 * This class should be implemented by a user-defined job
 * which specify a data parser and tasks composing the job
 */
public interface UserJobInfo {

    public abstract List<UserTaskInfo> getTaskInfoList();

    public abstract Class<? extends DataParser> getDataParser();

}
