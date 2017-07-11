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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Using Apache HTTP Network Service, it sends HTTP requests to specified URL.
 */
final class HttpSender {

  private static final int DIVIDE_LENGTH = 2000;
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
    Logger.getLogger(HttpSender.class.getName()).log(Level.INFO, "serializedJobConf is : {0}", serializedJobConf);
    final HttpClient httpClient = HttpClientBuilder.create().build();
    final String url = "http://" + address + ":" + port + "/dolphin/v1/" + Parameters.SUBMIT_COMMAND;
    // using time stamp for connection protocol header
    final long header = System.currentTimeMillis();
    final Pair<String, String> subConfPair =
        Pair.of(String.valueOf(header) + serializedJobConf.substring(0, DIVIDE_LENGTH),
            String.valueOf(header) + serializedJobConf.substring(DIVIDE_LENGTH));

    final HttpPost submitRequest = new HttpPost(url);
    ByteArrayEntity entity = new ByteArrayEntity(subConfPair.getLeft().getBytes());
    HttpResponse response;
    try {
      submitRequest.setEntity(entity);
      response = httpClient.execute(submitRequest);
      System.out.println("\nSending 'POST' request to URL : " + url);
      System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
          ", Response Message : " + response.getStatusLine().getReasonPhrase());

      if (response.getStatusLine().getStatusCode() == 200) {
        entity = new ByteArrayEntity(subConfPair.getRight().getBytes());
        submitRequest.setEntity(entity);
        response = httpClient.execute(submitRequest);
        System.out.println("\nSending 'POST' request to URL : " + url);
        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
            ", Response Message : " + response.getStatusLine().getReasonPhrase());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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
