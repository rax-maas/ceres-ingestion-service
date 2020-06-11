package com.rackspacecloud.metrics.ingestionservice.exceptions;

/**
 * This exception is thrown when route information is not found for the given tenantId.
 */
public class RouteNotFoundException extends RuntimeException {
  public RouteNotFoundException() {
    super("Metric route not found");
  }

  public RouteNotFoundException(Throwable e) {
      super("Metric route not found", e);
  }
}
