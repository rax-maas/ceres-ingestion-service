package com.rackspacecloud.metrics.kafkainfluxdbconsumer.provider;

public interface IAuthTokenProvider {
    String getTenantId();
    String getAuthToken();
}
