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

  /**
   * Sends a submit command.
   * @param address an address of HTTP request
   * @param port a port number of HTTP request
   * @param serializedConf a job configuration for submitting a job.
   *                       It is serialized to send via HTTP POST body parameters.
   */
  public static void sendSubmitCommand(final String address, final String port, final String serializedConf) {
    final HttpClient httpClient = HttpClientBuilder.create().build();
    final String url = "http://" + address + ":" + port + "/dolphin/v1/" + Parameters.SUBMIT_COMMAMD;

    final NameValuePair confPair = new BasicNameValuePair("conf", serializedConf);
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
  public static void sendFinishCommand(final String address, final String port) {
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
