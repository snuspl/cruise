package org.apache.reef.inmemory.driver.service;

import com.microsoft.reef.webserver.HttpHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Registers host and port information to YARN RM's tracking URL.
 *
 * Accessing http://{YARN RM tracking URL}/surf/v1 returns the
 * Host and Port in plain text.
 *
 * The AddressHttpHandler must be registered as an HttpHandler on REEF's
 * HttpServer.
 */
public final class YarnServiceRegistry implements ServiceRegistry {

  private final AddressHttpHandler httpHandler;

  @Inject
  public YarnServiceRegistry(final AddressHttpHandler httpHandler) {
    this.httpHandler = httpHandler;
  }

  public static final class AddressHttpHandler implements HttpHandler {

    private static final String uriSpecification = "surf";

    private String host;
    private int port;

    @Inject
    public AddressHttpHandler() {
    }

    public void setHost(String host) {
      this.host = host;
    }

    public void setPort(int port) {
      this.port = port;
    }

    @Override
    public String getUriSpecification() {
      return uriSpecification;
    }

    @Override
    public void setUriSpecification(String s) {
      // Don't set
    }

    @Override
    public void onHttpRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
      response.setContentType("text/plain");

      final PrintWriter responseWriter = response.getWriter();
      responseWriter.printf("%s:%d", host, port);
      responseWriter.flush();
    }
  }

  @Override
  public void register(final String host, final int port) {
    httpHandler.setHost(host);
    httpHandler.setPort(port);
  }
}
