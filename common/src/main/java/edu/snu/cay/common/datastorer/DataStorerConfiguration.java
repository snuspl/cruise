package edu.snu.cay.common.datastorer;

import edu.snu.cay.common.datastorer.param.BaseDir;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Builds configuration for DataStorer service.
 */
public final class DataStorerConfiguration extends ConfigurationModuleBuilder {

  /**
   * The base directory of the files
   */
  public static final RequiredParameter<String> BASE_DIR = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new DataStorerConfiguration()
      .bindNamedParameter(BaseDir.class, BASE_DIR)
      .build();
}
