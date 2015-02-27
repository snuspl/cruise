package edu.snu.reef.flexion.core;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(UserParametersImpl.class)
public interface UserParameters {

    public Configuration getDriverConf();

    public Configuration getUserCmpTaskConf();

    public Configuration getUserCtrlTaskConf();

}
