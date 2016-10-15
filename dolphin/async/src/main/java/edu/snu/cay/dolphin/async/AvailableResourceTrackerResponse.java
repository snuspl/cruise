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

/**
 * This class specifies the response from the {@link AvailableResourceTracker}.
 */
final class AvailableResourceTrackerResponse {
  /**
   * 200 OK : The request succeeded normally.
   */
  private static final int ART_OK = 200;

  /**
   * 400 BAD REQUEST : The request is syntactically incorrect.
   */
  private static final int ART_BAD_REQUEST = 400;

  /**
   * 403 FORBIDDEN : Syntactically okay but refused to process.
   */
  private static final int ART_FORBIDDEN = 403;

  /**
   * 404 NOT FOUND :  The resource is not available.
   */
  private static final int ART_NOT_FOUND = 404;

  /**
   * Create a response with OK status.
   */
  public static AvailableResourceTrackerResponse ok(final String message) {
    return new AvailableResourceTrackerResponse(ART_OK, message);
  }

  /**
   * Create a response with BAD_REQUEST status.
   */
  public static AvailableResourceTrackerResponse badRequest(final String message) {
    return new AvailableResourceTrackerResponse(ART_BAD_REQUEST, message);
  }

  /**
   * Create a response with FORBIDDEN status.
   */
  public static AvailableResourceTrackerResponse forbidden(final String message) {
    return new AvailableResourceTrackerResponse(ART_FORBIDDEN, message);
  }

  /**
   * Create a response with NOT FOUND status.
   */
  public static AvailableResourceTrackerResponse notFound(final String message) {
    return new AvailableResourceTrackerResponse(ART_NOT_FOUND, message);
  }
  /**
   * Return {@code true} if the response is OK.
   */
  public boolean isOK() {
    return this.status == ART_OK;
  }

  /**
   * Status code of the request based on RFC 2068.
   */
  private int status;

  /**
   * Message to send.
   */
  private String message;

  /**
   * Constructor using status code and message.
   * @param status
   * @param message
   */
  private AvailableResourceTrackerResponse(final int status, final String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * Return the status code of this response.
   */
  int getStatus() {
    return status;
  }

  /**
   * Return the message of this response.
   */
  String getMessage() {
    return message;
  }
}
