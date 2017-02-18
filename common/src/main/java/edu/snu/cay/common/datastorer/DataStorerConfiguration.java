package edu.snu.cay.common.datastorer;

import edu.snu.cay.common.datastorer.param.BaseDir;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Created by yunseong on 2/13/17.
 */
public final class DataStorerConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredParameter<String> BASE_DIR = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new DataStorerConfiguration()
      .bindNamedParameter(BaseDir.class, BASE_DIR)
      .build();
}
