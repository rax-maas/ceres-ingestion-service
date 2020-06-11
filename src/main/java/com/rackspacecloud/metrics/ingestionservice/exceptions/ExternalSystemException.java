package com.rackspacecloud.metrics.ingestionservice.exceptions;

public class ExternalSystemException extends RuntimeException {
  public ExternalSystemException(Throwable throwable) {
    super(throwable);
  }
}
