package org.springframework.data.influxdb;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.WriteConsistency;
import com.influxdb.client.domain.WritePrecision;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author baisongyuan
 * @className InfluxDBConnectionFactory
 * @description
 * @date 2022/4/28 10:16
 */
public class InfluxDBConnectionFactory implements InitializingBean {

    private final InfluxDBProperties influxDbProperties;

    private InfluxDBClient influxDbClient;

    private InfluxDBClientOptions influxDBClientOptions;

    private InfluxDBClientFactory influxDBClientFactory;

    public InfluxDBConnectionFactory(InfluxDBProperties influxDbProperties) {
        this.influxDbProperties = influxDbProperties;
    }

    public InfluxDBClient getInfluxDBClient(){
        if (Objects.isNull(influxDbClient)){
            synchronized (InfluxDBConnectionFactory.class){
                if (Objects.isNull(influxDbClient)){
                    this.influxDbClient = InfluxDBClientFactory.create(influxDBClientOptions);
                }
            }
        }
        return influxDbClient;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        InfluxDBClientOptions.Builder optionBuilder = InfluxDBClientOptions.builder();
        String url = influxDbProperties.getUrl();
        if (StringUtils.hasText(url)){
            optionBuilder.url(url);
        }
        String org = influxDbProperties.getOrg();
        if (StringUtils.hasText(org)){
            optionBuilder.org(org);
        }
        String token = influxDbProperties.getToken();
        if (StringUtils.hasText(token)){
            optionBuilder.authenticateToken(token.toCharArray());
        }else{
            String username = influxDbProperties.getUsername();
            String password = influxDbProperties.getPassword();
            optionBuilder.authenticateToken(String.format("%s:%s",
                    username == null ? "" : username,
                    password == null ? "" : password).toCharArray());

        }
        String bucket = influxDbProperties.getBucket();
        String database = influxDbProperties.getDatabase();
        if (StringUtils.hasText(bucket)){
            optionBuilder.bucket(bucket);
        }else if (StringUtils.hasText(database)){
            String retentionPolicy = influxDbProperties.getRetentionPolicy();
            optionBuilder.bucket(String.format("%s/%s", database, retentionPolicy == null ? "" : retentionPolicy));
        }
        if (StringUtils.hasText(influxDbProperties.getWriteConsistency())){
            WriteConsistency writeConsistency = WriteConsistency.fromValue(influxDbProperties.getWriteConsistency());
            optionBuilder.consistency(writeConsistency);
        }
        if (StringUtils.hasText(influxDbProperties.getLogLevel())){
            String logLevel = influxDbProperties.getLogLevel();
            optionBuilder.logLevel(LogLevel.valueOf(logLevel));
        }
        String precision = influxDbProperties.getPrecision();
        if (StringUtils.hasText(precision)){
            optionBuilder.precision(WritePrecision.valueOf(precision));
        }
        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
                .protocols(Collections.singletonList(Protocol.HTTP_1_1))
                .connectTimeout(influxDbProperties.getConnectionTimeout(), TimeUnit.SECONDS)
                .writeTimeout(influxDbProperties.getWriteTimeout(), TimeUnit.SECONDS)
                .readTimeout(influxDbProperties.getReadTimeout(), TimeUnit.SECONDS);

        optionBuilder.okHttpClient(okHttpClientBuilder);
        this.influxDBClientOptions = optionBuilder.build();
    }
}
