package edu.snu.reef.flexion.core;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for providing configurations setting user-defined (algorithmic-specific, application-specific) parameters
 */
@DefaultImplementation(UserParametersImpl.class)
public interface UserParameters {

    public Configuration getDriverConf();

    public Configuration getServiceConf();

    public Configuration getUserCmpTaskConf();

    public Configuration getUserCtrlTaskConf();

}
