package edu.snu.reef.flexion.core;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;


/**
 * Default implementation of {@link edu.snu.reef.flexion.core.UserParameters} which returns empty configurations.
 */
public final class UserParametersImpl implements UserParameters {

  @Inject
  public UserParametersImpl() {

  }

  @Override
  public Configuration getDriverConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

  @Override
  public Configuration getUserCmpTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }

}