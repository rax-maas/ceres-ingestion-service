package com.rackspacecloud.metrics.ingestionservice.exceptions;

public class InvalidDataException extends IngestFailedException {

  public InvalidDataException(String s) {
    super(s);
  }

  public InvalidDataException(Throwable throwable) {
    super(throwable);
  }
}
