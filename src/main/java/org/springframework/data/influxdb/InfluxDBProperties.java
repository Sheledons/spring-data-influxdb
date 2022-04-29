package org.springframework.data.influxdb;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author baisongyuan
 * @className InfluxProperties
 * @description
 * @date 2022/4/28 10:07
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "spring.influxdb")
public class InfluxDBProperties {
    private String url;
    private String token;
    private String org;
    private String bucket;
    private String username;
    private String password;
    private String retentionPolicy;
    private String writeConsistency;
    private String database;
    private String logLevel;
    private String precision;
    private int connectionTimeout = 10;
    private int readTimeout = 30;
    private int writeTimeout = 10;
}
