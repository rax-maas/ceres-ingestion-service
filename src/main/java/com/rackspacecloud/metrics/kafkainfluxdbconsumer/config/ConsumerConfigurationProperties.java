package com.rackspacecloud.metrics.kafkainfluxdbconsumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.List;

@ConfigurationProperties("kafka")
public class ConsumerConfigurationProperties {
    private List<String> servers;
    private long listenerContainerIdleInterval;
    private int sessionTimeoutMsConfig;

    public int getSessionTimeoutMsConfig() {
        return sessionTimeoutMsConfig;
    }

    public void setSessionTimeoutMsConfig(int sessionTimeoutMsConfig) {
        this.sessionTimeoutMsConfig = sessionTimeoutMsConfig;
    }

    public List<String> getServers(){
        return servers;
    }

    public void setServers(String servers){
        this.servers = Arrays.asList(servers.split(";"));
    }

    public long getListenerContainerIdleInterval() {
        return listenerContainerIdleInterval;
    }

    public void setListenerContainerIdleInterval(long listenerContainerIdleInterval) {
        this.listenerContainerIdleInterval = listenerContainerIdleInterval;
    }

    private Properties properties = new Properties();

    public Properties getProperties(){
        return properties;
    }

    private Ssl ssl = new Ssl();

    public Ssl getSsl(){
        return ssl;
    }

    private Consumer consumer = new Consumer();

    public Consumer getConsumer() {
        return consumer;
    }

    public static class Consumer{
        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        private String group;
    }

    public static class Ssl {
        private String truststoreLocation;

        public String getTruststoreLocation(){
            return truststoreLocation;
        }

        public void setTruststoreLocation(String truststoreLocation){
            this.truststoreLocation = truststoreLocation;
        }

        private String truststorePassword;

        public String getTruststorePassword(){
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword){
            this.truststorePassword = truststorePassword;
        }

        private String keystoreLocation;

        public String getKeystoreLocation(){
            return keystoreLocation;
        }

        public void setKeystoreLocation(String keystoreLocation){
            this.keystoreLocation = keystoreLocation;
        }

        private String keystorePassword;

        public String getKeystorePassword(){
            return keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword){
            this.keystorePassword = keystorePassword;
        }

        private String keyPassword;

        public String getKeyPassword(){
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword){
            this.keyPassword = keyPassword;
        }
    }

    public static class Properties{
        private String securityProtocol;

        public String getSecurityProtocol(){
            return securityProtocol;
        }

        public void setSecurityProtocol(String securityProtocol){
            this.securityProtocol = securityProtocol;
        }
    }
}
