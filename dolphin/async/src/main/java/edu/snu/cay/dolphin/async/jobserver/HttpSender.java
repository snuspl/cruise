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

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * Using Apache HTTP Network Service, it sends HTTP requests to specified URL.
 */
final class HttpSender {

  private HttpSender() {

  }

  /**
   * Sends a submit command.
   * @param address an address of HTTP request
   * @param port a port number of HTTP request
   * @param serializedJobConf a job configuration for submitting a job.
   *                       It is serialized to send via HTTP POST body parameters.
   */
  static void sendSubmitCommand(final String address, final String port, final String serializedJobConf) {
    final HttpClient httpClient = HttpClientBuilder.create().build();
    final String url = "http://" + address + ":" + port + "/dolphin/v1/" + Parameters.SUBMIT_COMMAMD;

    final NameValuePair confPair = new BasicNameValuePair("conf", serializedJobConf);
    final List<NameValuePair> nameValuePairs = Collections.singletonList(confPair);
    final HttpPost submitRequest = new HttpPost(url);
    try {
      submitRequest.setEntity(new UrlEncodedFormEntity(nameValuePairs));
      submitRequest.addHeader("content-type", "application/x-www-form-urlencoded");
      final HttpResponse response = httpClient.execute(submitRequest);
      System.out.println("\nSending 'POST' request to URL : " + url);

      System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
          ", Response Message : " + response.getStatusLine().getReasonPhrase());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Sends a finish command.
   * @param address an address of HTTP request
   * @param port a port number of HTTP request
   */
  static void sendFinishCommand(final String address, final String port) {
    final HttpClient httpClient = HttpClientBuilder.create().build();
    final String url = "http://" + address + ":" + port + "/dolphin/v1/" + Parameters.FINISH_COMMAND;

    final HttpGet finishRequest = new HttpGet(url);
    try {
      final HttpResponse response = httpClient.execute(finishRequest);
      System.out.println("\nSending 'GET' request to URL : " + url);

      System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
          ", Response Message : " + response.getStatusLine().getReasonPhrase());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
