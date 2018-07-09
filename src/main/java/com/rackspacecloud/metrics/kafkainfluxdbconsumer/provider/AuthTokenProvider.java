package com.rackspacecloud.metrics.kafkainfluxdbconsumer.provider;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.metrics.kafkainfluxdbconsumer.config.RestTemplateConfigurationProperties;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AuthTokenProvider implements IAuthTokenProvider {
    RestTemplate restTemplate;
    RestTemplateConfigurationProperties config;
    private String authToken;
    private String tenantId;
    private long authTokenExpirationTime;
    private final long secondsToTokenExpirationTime;

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthTokenProvider.class);

    @Autowired
    public AuthTokenProvider(RestTemplate restTemplate, RestTemplateConfigurationProperties config){
        this.restTemplate = restTemplate;
        this.config = config;
        this.authToken = "";
        this.secondsToTokenExpirationTime = config.getSecondsToTokenExpirationTime();
        setAuthTokenAndTenantId();
    }

    private void setAuthTokenAndTenantId()
    {
        Credential credential = new Credential();
        credential.setUsername(config.getCredential().getUser());
        credential.setApiKey(config.getCredential().getApiKey());
        Auth auth = new Auth();
        auth.setCredential(credential);
        Identity identity = new Identity();
        identity.setAuth(auth);

        ObjectMapper mapper = new ObjectMapper();

        try {
            String authString = mapper.writeValueAsString(identity);
            HttpEntity<String> entity = new HttpEntity<>(authString, getHttpHeaders());
            ResponseEntity<IdentityAccess> response =
                    restTemplate.exchange(config.getCredential().getIdentityUrl(),
                            HttpMethod.POST, entity, IdentityAccess.class);

            // Not doing any null check here because it's getting validated when it's deserialized
            Token token = response.getBody().access.token;

            authToken = token.id;
            tenantId = token.tenant.id;

            // Not doing any null check here because it's getting validated when it's deserialized
            authTokenExpirationTime = response.getBody().getAccess().getToken().getExpires().getTime();

        } catch (JsonProcessingException e) {
            LOGGER.error("Cannot process json. Exception message: {}", e.getMessage());
        }
    }

    @Override
    public String getAuthToken(){
        // Return existing auth-token IF:
        //      token's expiration date is more than margin seconds away from now
        //      AND auth-token is not null
        // If current time is within the margin or beyond expiration time, then refresh the token
        if(Instant.now().plusSeconds(secondsToTokenExpirationTime).toEpochMilli() < authTokenExpirationTime
                && !StringUtils.isEmpty(authToken)) return authToken;

        setAuthTokenAndTenantId();
        return authToken;
    }

    @Override
    public String getTenantId(){
        return tenantId;
    }

    private HttpHeaders getHttpHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.APPLICATION_JSON);
        headers.setAccept(mediaTypes);

        return headers;
    }

    @Data
    static class Identity {
        Auth auth;
    }

    @Data
    static class Auth {
        @JsonProperty("RAX-KSKEY:apiKeyCredentials")
        Credential credential;
    }

    @Data
    static class Credential {
        String username;
        String apiKey;
    }

    @Data
    static class IdentityAccess {
        Access access;
    }

    @Data
    static class Access {
        List<ServiceCatalogItem> serviceCatalog;
        User user;
        Token token;
    }

    @Data
    static class ServiceCatalogItem {
        List<Endpoint> endpoints;
        String name;
        String type;
    }

    @Data
    static class Endpoint {
        String tenantId;
        String publicURL;
        String region;
    }

    @Data
    static class User {
        @JsonProperty("RAX-AUTH:sessionInactivityTimeout")
        String sessionInactivityTimeout;

        @JsonProperty("RAX-AUTH:defaultRegion")
        String defaultRegion;

        @JsonProperty("RAX-AUTH:domainId")
        String domainId;

        String id;
        String name;

        List<Role> roles;
    }

    @Data
    static class Role {
        String name;
        String description;
        String id;
    }

    @Data
    static class Token {
        Date expires;

        @JsonProperty("RAX-AUTH:issued")
        Date issued;

        @JsonProperty("RAX-AUTH:authenticatedBy")
        List<String> authenticatedBy;
        String id;
        Tenant tenant;
    }

    @Data
    static class Tenant {
        String name;
        String id;
    }
}
