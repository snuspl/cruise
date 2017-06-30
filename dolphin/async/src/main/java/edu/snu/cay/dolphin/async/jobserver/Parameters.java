package edu.snu.cay.dolphin.async.jobserver;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by xyzi on 30/06/2017.
 */
public class Parameters {
  public static final String SUBMIT_COMMAMD = "submit";
  public static final String FINISH_COMMAND = "finish";

  @NamedParameter(doc = "A port number of HTTP request.", short_name = "port")
  public final class HttpPort implements Name<String> {

  }

  @NamedParameter(doc = "An address of HTTP request", short_name = "address")
  public final class HttpAddress implements Name<String> {

  }

  @NamedParameter(doc = "An identifier of App.")
  public final class AppIdentifier implements Name<String> {

  }
}
