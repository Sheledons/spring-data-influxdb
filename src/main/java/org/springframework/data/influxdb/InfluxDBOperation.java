package org.springframework.data.influxdb;

import com.influxdb.client.BucketsApi;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
/**
 * @author baisongyuan
 * @className InfluxDBOperation
 * @description
 * @date 2022/4/28 10:30
 */
public interface InfluxDBOperation<T> extends WriteApiBlocking, QueryApi, BucketsApi {
}
