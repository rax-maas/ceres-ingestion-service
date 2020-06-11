package com.rackspacecloud.metrics.ingestionservice.exceptions;

public class IngestFailedException extends Exception {
  public IngestFailedException(String s) {
    super(s);
  }

  public IngestFailedException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public IngestFailedException(Throwable throwable) {
    super(throwable);
  }
}
