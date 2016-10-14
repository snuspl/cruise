/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.param.Parameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * A module for tracking changes in the number of available resources.
 * it updates the number on http request
 * and provides it via {@link AvailableResourceTracker#getNumAvailableResources()}.
 */
public final class AvailableResourceTrackerImpl implements AvailableResourceTracker {
  private AvailableResourceListener listener;

  @Inject
  private AvailableResourceTrackerImpl(
      final AvailableResourceListener listener) {
    this.listener = listener;
  }

  /**
   * Returns the total number of resources currently available.
   * @return the total number of available resources.
   */
  @Override
  public int getNumAvailableResources() {
    return listener.getNumAvailableResources();
  }

  public static Configuration getHttpConf() {
    final Configuration httpHandlerConf = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, AvailableResourceListener.class)
        .build();
    return httpHandlerConf;
  }

  /**
   * A listener who receives http requests for updating the number of resources currently available.
   */
  public static class AvailableResourceListener implements HttpHandler {
    private String uriSpecification = "dolphin-available-resource-tracker";
    private int numAvailableResources;

    @Inject
    AvailableResourceListener(@Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class)
                              final int numAvailableResources) {
      this.numAvailableResources = numAvailableResources;
    }

    @Override
    public String getUriSpecification() {
      return uriSpecification;
    }

    @Override
    public void setUriSpecification(final String s) {
      uriSpecification = s;
    }

    /**
     * HttpRequest handler. You must specify UriSpecification and API version.
     * The request url is http://{address}:{port}/scheduler/v1
     * <p>
     * APIs
     * /update                to update the number of available resources
     */
    @Override
    public void onHttpRequest(final ParsedHttpRequest request, final HttpServletResponse response)
        throws IOException, ServletException {
      final String target = request.getTargetEntity().toLowerCase();

      switch (target) {
      case "update":
        final List<String> args = request.getQueryMap().get("numAvailableResources");
        final int updatedNumAvailableResources = Integer.parseInt(args.get(0));
        if (updatedNumAvailableResources < 0) {
          response.getWriter().println(String.format("Invalid value of numAvailableResources: [%s]",
              updatedNumAvailableResources));
          break;
        }
        numAvailableResources = updatedNumAvailableResources;
        break;
      default:
        response.getWriter().println(String.format("Unsupported operation: [%s].", target));
      }
    }

    int getNumAvailableResources() {
      return numAvailableResources;
    }

  }
}
