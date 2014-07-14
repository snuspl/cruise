package org.apache.reef.inmemory.driver.service;

import com.microsoft.reef.webserver.HttpHandler;
import com.microsoft.reef.webserver.HttpServer;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public final class YarnServiceRegistry implements ServiceRegistry {

  private final HttpServer httpServer;

  @Inject
  public YarnServiceRegistry(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  private static final class AddressHttpHandler implements HttpHandler {

    private static final String uriSpecification = "surf";

    private final String host;
    private final int port;

    public AddressHttpHandler(final String host,
                              final int port) {
      this.host = host;
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
      response.setContentType("application/json");

      final PrintWriter responseWriter = response.getWriter();
      responseWriter.printf("{ %s : \"%s:%d\"}", "address", host, port);
      responseWriter.flush();
    }
  }

  @Override
  public void register(final String address, final int port) {
    httpServer.addHttpHandler(new AddressHttpHandler(address, port));
  }
}
