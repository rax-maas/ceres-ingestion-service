package com.rackspacecloud.metrics.ingestionservice.exceptions;

public class QueryFailedException extends RuntimeException {
  public QueryFailedException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public QueryFailedException() {
    super("Unable to query for database existence");
  }
}
