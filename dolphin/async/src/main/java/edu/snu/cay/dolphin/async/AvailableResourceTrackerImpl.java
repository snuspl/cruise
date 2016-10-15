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
  private HttpHandlerImpl listener;

  @Inject
  private AvailableResourceTrackerImpl(
      final HttpHandlerImpl listener) {
    this.listener = listener;
  }

  /**
   * @return the total number of available resources.
   */
  @Override
  public int getNumAvailableResources() {
    return listener.getNumAvailableResources();
  }

  /**
   * @return a configuration where {@link HttpHandlerImpl} is added to {@link HttpHandlerConfiguration#HTTP_HANDLERS}.
   */
  public static Configuration getHttpConf() {
    final Configuration httpHandlerConf = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpHandlerImpl.class)
        .build();
    return httpHandlerConf;
  }

  /**
   * A listener who receives http requests for updating the number of resources currently available.
   */
  private static class HttpHandlerImpl implements HttpHandler {
    private String uriSpecification = "available-resource-tracker";
    private int numAvailableResources;

    @Inject
    HttpHandlerImpl(@Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class)
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
     * The request url is http://{address}:{port}/available-resource-tracker/v1
     * <p>
     * APIs
     * /update                to update the number of available resources
     */
    @Override
    public void onHttpRequest(final ParsedHttpRequest request, final HttpServletResponse response)
        throws IOException, ServletException {
      AvailableResourceTrackerResponse result;
      final String target = request.getTargetEntity().toLowerCase();

      switch (target) {
      case "update":
        final List<String> args = request.getQueryMap().get("numAvailableResources");
        final int updatedNumAvailableResources = Integer.parseInt(args.get(0));
        if (updatedNumAvailableResources < 0) {
          result = AvailableResourceTrackerResponse.badRequest("Invalid value of numAvailableResources");
          break;
        }

        numAvailableResources = updatedNumAvailableResources;
        result = AvailableResourceTrackerResponse.ok("numAvailableResources updated to" + numAvailableResources);
        break;
      default:
        result = AvailableResourceTrackerResponse.notFound("Unsupported operation");
      }

      response.sendError(result.getStatus(), result.getMessage());

    }

    public int getNumAvailableResources() {
      return numAvailableResources;
    }
  }
}
