package edu.snu.cay.common.datastorer.param;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by yunseong on 2/13/17.
 */
@NamedParameter(doc = "The base directory of the files", short_name = "base_dir")
public final class BaseDir implements Name<String> {
}
