/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.jobserver;

import com.google.gson.Gson;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Receive HttpRequest so that it can handle the command list.
 */
public final class JobServerHttpHandler implements HttpHandler {

  private String uriSpecification = "dolphin";
  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  private JobServerHttpHandler(final InjectionFuture<JobServerDriver> jobServerDriverFuture,
                               final ConfigurationSerializer configurationSerializer) {
    this.jobServerDriverFuture = jobServerDriverFuture;
    this.configurationSerializer = configurationSerializer;
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(final String newSpecification) {
    uriSpecification = newSpecification;
  }

  /**
   * HttpRequest handler.
   * The request url is http://127.0.1.1:{port}/dolphin/v1/{command}
   *
   * APIs
   *    /submit?conf={"jobConf" : confString}     to submit a new job.
   *    /finish                                   to finish the job server.
   */
  @Override
  public void onHttpRequest(final ParsedHttpRequest request, final HttpServletResponse httpServletResponse)
      throws IOException, ServletException {

    final String target = request.getTargetEntity().toLowerCase();
    final Map<String, List<String>> queryMap = request.getQueryMap();
    final HttpResponse result;
    switch (target) {
    case "submit":
      try {
        result = onSubmit(queryMap);
      } catch (InjectionException e) {
        throw new RuntimeException(e);
      }
      break;
    case "finish":
      result = onFinish();
      break;
    default:
      return;
    }

    final int status = result.getStatus();
    final String message = result.getMessage();

    if (result.isOK()) {
      httpServletResponse.getOutputStream().println(message);
    } else {
      httpServletResponse.sendError(status, message);
    }
  }

  private HttpResponse onSubmit(final Map<String, List<String>> queryMap) throws IOException, InjectionException {
    final List<String> args = queryMap.get("conf");
    final Gson gson = new Gson();
    if (args.size() != 1) {
      return HttpResponse.badRequest("Usage : only one configuration at a time");
    } else {
      final String serializedJobConf = gson.fromJson(args.get(0), JobRequest.class).getJobConf();
      final Configuration jobConf = configurationSerializer.fromString(serializedJobConf);
      jobServerDriverFuture.get().executeJob(jobConf);
      return HttpResponse.ok("Job is successfully submitted");
    }
  }

  private HttpResponse onFinish() {
    jobServerDriverFuture.get().finishServer();
    return HttpResponse.ok("Job server is successfully finished");
  }

  private final class JobRequest {

    private String jobConf;

    String getJobConf() {
      return jobConf;
    }
  }
}
