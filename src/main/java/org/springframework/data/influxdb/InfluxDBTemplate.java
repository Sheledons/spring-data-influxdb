package org.springframework.data.influxdb;

import com.influxdb.Cancellable;
import com.influxdb.client.BucketsApi;
import com.influxdb.client.BucketsQuery;
import com.influxdb.client.FindOptions;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.OrganizationsApi;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.client.domain.Buckets;
import com.influxdb.client.domain.Dialect;
import com.influxdb.client.domain.Label;
import com.influxdb.client.domain.LabelResponse;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.PostBucketRequest;
import com.influxdb.client.domain.Query;
import com.influxdb.client.domain.ResourceMember;
import com.influxdb.client.domain.ResourceOwner;
import com.influxdb.client.domain.User;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.client.write.WriteParameters;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author baisongyuan
 * @className InfluxDBTemplate
 * @description
 * @date 2022/4/28 10:27
 */
public class InfluxDBTemplate<T> implements InfluxDBOperation<T>{

    private InfluxDBConnectionFactory connectionFactory;

    public InfluxDBTemplate(InfluxDBConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void writeRecord(@Nonnull WritePrecision precision, @Nullable String record) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeRecord(precision,record);
    }

    @Override
    public void writeRecord(@Nonnull String bucket, @Nonnull String org, @Nonnull WritePrecision precision, @Nullable String record) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeRecord(bucket, org, precision, record);
    }

    @Override
    public void writeRecord(@Nullable String record, @Nonnull WriteParameters parameters) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeRecord(record,parameters);
    }

    @Override
    public void writeRecords(@Nonnull WritePrecision precision, @Nonnull List<String> records) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeRecords(precision, records);
    }

    @Override
    public void writeRecords(@Nonnull String bucket, @Nonnull String org, @Nonnull WritePrecision precision, @Nonnull List<String> records) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeRecords(bucket, org, precision, records);
    }

    @Override
    public void writeRecords(@Nonnull List<String> records, @Nonnull WriteParameters parameters) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeRecords(records, parameters);
    }

    @Override
    public void writePoint(@Nullable Point point) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writePoint(point);
    }

    @Override
    public void writePoint(@Nonnull String bucket, @Nonnull String org, @Nullable Point point) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writePoint(bucket, org, point);
    }

    @Override
    public void writePoint(@Nullable Point point, @Nonnull WriteParameters parameters) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writePoint(point,parameters);
    }

    @Override
    public void writePoints(@Nonnull List<Point> points) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writePoints(points);
    }

    @Override
    public void writePoints(@Nonnull String bucket, @Nonnull String org, @Nonnull List<Point> points) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writePoints(bucket, org, points);
    }

    @Override
    public void writePoints(@Nonnull List<Point> points, @Nonnull WriteParameters parameters) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writePoints(points, parameters);
    }

    @Override
    public <M> void writeMeasurement(@Nonnull WritePrecision precision, @Nullable M measurement) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeMeasurement(precision,measurement);
    }

    @Override
    public <M> void writeMeasurement(@Nonnull String bucket, @Nonnull String org, @Nonnull WritePrecision precision, @Nullable M measurement) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeMeasurement(bucket,org,precision,measurement);
    }

    @Override
    public <M> void writeMeasurement(@Nullable M measurement, @Nonnull WriteParameters parameters) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeMeasurement(measurement,parameters);
    }

    @Override
    public <M> void writeMeasurements(@Nonnull WritePrecision precision, @Nonnull List<M> measurements) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeMeasurement(precision,measurements);
    }

    @Override
    public <M> void writeMeasurements(@Nonnull String bucket, @Nonnull String org, @Nonnull WritePrecision precision, @Nonnull List<M> measurements) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeMeasurement(bucket,org,precision,measurements);
    }

    @Override
    public <M> void writeMeasurements(@Nonnull List<M> measurements, @Nonnull WriteParameters parameters) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        WriteApiBlocking writeBlock = client.getWriteApiBlocking();
        writeBlock.writeMeasurement(measurements,parameters);
    }

    @Nonnull
    @Override
    public List<FluxTable> query(@Nonnull String query) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query);
    }

    @Nonnull
    @Override
    public List<FluxTable> query(@Nonnull String query, @Nonnull String org) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,org);
    }

    @Nonnull
    @Override
    public List<FluxTable> query(@Nonnull String query, @Nonnull String org, @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,org,params);
    }

    @Nonnull
    @Override
    public List<FluxTable> query(@Nonnull Query query) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query);
    }

    @Nonnull
    @Override
    public List<FluxTable> query(@Nonnull Query query, @Nonnull String org) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,org);
    }

    @Nonnull
    @Override
    public <M> List<M> query(@Nonnull String query, @Nonnull Class<M> measurementType) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,measurementType);
    }

    @Nonnull
    @Override
    public <M> List<M> query(@Nonnull String query, @Nonnull String org, @Nonnull Class<M> measurementType) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,org,measurementType);
    }

    @Override
    public <M> List<M> query(@Nonnull String query, @Nonnull String org, @Nonnull Class<M> measurementType, @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,org,measurementType,params);
    }

    @Nonnull
    @Override
    public <M> List<M> query(@Nonnull Query query, @Nonnull Class<M> measurementType) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,measurementType);
    }

    @Nonnull
    @Override
    public <M> List<M> query(@Nonnull Query query, @Nonnull String org, @Nonnull Class<M> measurementType) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.query(query,measurementType);
    }

    @Override
    public void query(@Nonnull String query, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,onNext);
    }

    @Override
    public void query(@Nonnull String query, @Nonnull String org, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext);
    }

    @Override
    public void query(@Nonnull Query query, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,onNext);
    }

    @Override
    public void query(@Nonnull Query query, @Nonnull String org, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext);
    }

    @Override
    public <M> void query(@Nonnull String query, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,measurementType,onNext);
    }

    @Override
    public <M> void query(@Nonnull String query, @Nonnull String org, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext);
    }

    @Override
    public <M> void query(@Nonnull Query query, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,measurementType,onNext);
    }

    @Override
    public <M> void query(@Nonnull Query query, @Nonnull String org, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext);
    }

    @Override
    public void query(@Nonnull String query, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,onNext,onError);
    }

    @Override
    public void query(@Nonnull String query, @Nonnull String org, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext,onError);
    }

    @Override
    public void query(@Nonnull Query query, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,onNext,onError);
    }

    @Override
    public void query(@Nonnull Query query, @Nonnull String org, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext,onError);
    }

    @Override
    public <M> void query(@Nonnull String query, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,measurementType,onNext,onError);
    }

    @Override
    public <M> void query(@Nonnull String query, @Nonnull String org, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext,onError);
    }

    @Override
    public <M> void query(@Nonnull Query query, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,measurementType,onNext,onError);
    }

    @Override
    public <M> void query(@Nonnull Query query, @Nonnull String org, @Nonnull Class<M> measurementType, @Nonnull BiConsumer<Cancellable, M> onNext, @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext,onError);
    }

    @Override
    public void query(@Nonnull String query, @Nonnull BiConsumer<Cancellable, FluxRecord> onNext, @Nonnull Consumer<? super Throwable> onError, @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,onNext,onError,onComplete);
    }

    @Override
    public void query(@Nonnull String query,
                      @Nonnull String org,
                      @Nonnull BiConsumer<Cancellable, FluxRecord> onNext,
                      @Nonnull Consumer<? super Throwable> onError,
                      @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext,onError,onComplete);
    }

    @Override
    public void query(@Nonnull String query,
                      @Nonnull String org,
                      @Nonnull BiConsumer<Cancellable, FluxRecord> onNext,
                      @Nonnull Consumer<? super Throwable> onError,
                      @Nonnull Runnable onComplete,
                      @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext,onError,onComplete,params);
    }

    @Override
    public void query(@Nonnull Query query,
                      @Nonnull BiConsumer<Cancellable, FluxRecord> onNext,
                      @Nonnull Consumer<? super Throwable> onError,
                      @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,onNext,onError,onComplete);
    }

    @Override
    public void query(@Nonnull Query query,
                      @Nonnull String org,
                      @Nonnull BiConsumer<Cancellable, FluxRecord> onNext,
                      @Nonnull Consumer<? super Throwable> onError,
                      @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,onNext,onError,onComplete);
    }

    @Override
    public <M> void query(@Nonnull String query,
                          @Nonnull Class<M> measurementType,
                          @Nonnull BiConsumer<Cancellable, M> onNext,
                          @Nonnull Consumer<? super Throwable> onError,
                          @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,measurementType,onNext,onError,onComplete);
    }

    @Override
    public <M> void query(@Nonnull String query,
                          @Nonnull String org,
                          @Nonnull Class<M> measurementType,
                          @Nonnull BiConsumer<Cancellable, M> onNext,
                          @Nonnull Consumer<? super Throwable> onError,
                          @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext,onError,onComplete);
    }

    @Override
    public <M> void query(@Nonnull String query,
                          @Nonnull String org,
                          @Nonnull Class<M> measurementType,
                          @Nonnull BiConsumer<Cancellable, M> onNext,
                          @Nonnull Consumer<? super Throwable> onError,
                          @Nonnull Runnable onComplete,
                          @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext,onError,onComplete,params);
    }

    @Override
    public <M> void query(@Nonnull Query query,
                          @Nonnull Class<M> measurementType,
                          @Nonnull BiConsumer<Cancellable, M> onNext,
                          @Nonnull Consumer<? super Throwable> onError,
                          @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,measurementType,onNext,onError,onComplete);
    }

    @Override
    public <M> void query(@Nonnull Query query,
                          @Nonnull String org,
                          @Nonnull Class<M> measurementType,
                          @Nonnull BiConsumer<Cancellable, M> onNext,
                          @Nonnull Consumer<? super Throwable> onError,
                          @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.query(query,org,measurementType,onNext,onError,onComplete);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull String query) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull String query, @Nonnull String org) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query,org);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull String query, @Nullable Dialect dialect) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query,dialect);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull String query, @Nullable Dialect dialect, @Nonnull String org) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query,dialect,org);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull String query, @Nullable Dialect dialect, @Nonnull String org, @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query,dialect,org,params);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull Query query) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query);
    }

    @Nonnull
    @Override
    public String queryRaw(@Nonnull Query query, @Nonnull String org) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        return queryApi.queryRaw(query,org);
    }

    @Override
    public void queryRaw(@Nonnull String query, @Nonnull BiConsumer<Cancellable, String> onResponse) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,onResponse);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse);
    }

    @Override
    public void queryRaw(@Nonnull Query query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse);
    }

    @Override
    public void queryRaw(@Nonnull Query query, @Nonnull BiConsumer<Cancellable, String> onResponse) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,onResponse);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull BiConsumer<Cancellable, String> onResponse) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,onResponse);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,org,onResponse);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull Query query,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull Query query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,org,onResponse,onError);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse,onError,onComplete);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,onResponse,onError,onComplete);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,onResponse,onError,onComplete);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,org,onResponse,onError,onComplete);
    }

    @Override
    public void queryRaw(@Nonnull String query,
                         @Nullable Dialect dialect,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete,
                         @Nullable Map<String, Object> params) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,dialect,org,onResponse,onError,onComplete,params);
    }

    @Override
    public void queryRaw(@Nonnull Query query,
                         @Nonnull String org,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,org,onResponse,onError,onComplete);
    }

    @Override
    public void queryRaw(@Nonnull Query query,
                         @Nonnull BiConsumer<Cancellable, String> onResponse,
                         @Nonnull Consumer<? super Throwable> onError,
                         @Nonnull Runnable onComplete) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        QueryApi queryApi = client.getQueryApi();
        queryApi.queryRaw(query,onResponse,onError,onComplete);
    }

    @Nonnull
    @Override
    public Bucket createBucket(@Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(bucket);
    }

    @Nonnull
    @Override
    public Bucket createBucket(@Nonnull String name, @Nonnull Organization organization) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(name,organization);
    }

    @Nonnull
    @Override
    public Bucket createBucket(@Nonnull String name, @Nullable BucketRetentionRules bucketRetentionRules, @Nonnull Organization organization) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(name, bucketRetentionRules, organization);
    }

    @Nonnull
    @Override
    public Bucket createBucket(@Nonnull String name, @Nonnull String orgID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(name, orgID);
    }

    @Nonnull
    @Override
    public Bucket createBucket(@Nonnull String name, @Nullable BucketRetentionRules bucketRetentionRules, @Nonnull String orgID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(name, bucketRetentionRules, orgID);
    }

    @Nonnull
    @Override
    public Bucket createBucket(@Nonnull PostBucketRequest bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(bucket);
    }

    @Nonnull
    @Override
    public Bucket updateBucket(@Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.createBucket(bucket);
    }

    @Override
    public void deleteBucket(@Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteBucket(bucket);
    }

    @Override
    public void deleteBucket(@Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteBucket(bucketID);
    }

    @Nonnull
    @Override
    public Bucket cloneBucket(@Nonnull String clonedName, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.cloneBucket(clonedName,bucketID);
    }

    @Nonnull
    @Override
    public Bucket cloneBucket(@Nonnull String clonedName, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.cloneBucket(clonedName,bucket);
    }

    @Nonnull
    @Override
    public Bucket findBucketByID(@Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBucketByID(bucketID);
    }

    @Nullable
    @Override
    public Bucket findBucketByName(@Nonnull String bucketName) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBucketByName(bucketName);
    }

    @Nonnull
    @Override
    public List<Bucket> findBuckets() {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBuckets();
    }

    @Nonnull
    @Override
    public Buckets findBuckets(@Nonnull FindOptions findOptions) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBuckets(findOptions);
    }

    @Nonnull
    @Override
    public List<Bucket> findBucketsByOrg(@Nonnull Organization organization) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBucketsByOrg(organization);
    }

    @Nonnull
    @Override
    public List<Bucket> findBucketsByOrgName(@Nullable String orgName) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBucketsByOrgName(orgName);
    }

    @Nonnull
    @Override
    public List<Bucket> findBuckets(@Nonnull BucketsQuery query) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.findBuckets(query);
    }

    @Nonnull
    @Override
    public List<ResourceMember> getMembers(@Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.getMembers(bucket);
    }

    @Nonnull
    @Override
    public List<ResourceMember> getMembers(@Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.getMembers(bucketID);
    }

    @Nonnull
    @Override
    public ResourceMember addMember(@Nonnull User member, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.addMember(member,bucket);
    }

    @Nonnull
    @Override
    public ResourceMember addMember(@Nonnull String memberID, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.addMember(memberID,bucketID);
    }

    @Override
    public void deleteMember(@Nonnull User member, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteMember(member,bucket);
    }

    @Override
    public void deleteMember(@Nonnull String memberID, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteMember(memberID,bucketID);
    }

    @Nonnull
    @Override
    public List<ResourceOwner> getOwners(@Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.getOwners(bucket);
    }

    @Nonnull
    @Override
    public List<ResourceOwner> getOwners(@Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.getOwners(bucketID);
    }

    @Nonnull
    @Override
    public ResourceOwner addOwner(@Nonnull User owner, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.addOwner(owner,bucket);
    }

    @Nonnull
    @Override
    public ResourceOwner addOwner(@Nonnull String ownerID, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.addOwner(ownerID,bucketID);
    }

    @Override
    public void deleteOwner(@Nonnull User owner, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteOwner(owner,bucket);
    }

    @Override
    public void deleteOwner(@Nonnull String ownerID, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteOwner(ownerID,bucketID);
    }

    @Nonnull
    @Override
    public List<Label> getLabels(@Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.getLabels(bucket);
    }

    @Nonnull
    @Override
    public List<Label> getLabels(@Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.getLabels(bucketID);
    }

    @Nonnull
    @Override
    public LabelResponse addLabel(@Nonnull Label label, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.addLabel(label,bucket);
    }

    @Nonnull
    @Override
    public LabelResponse addLabel(@Nonnull String labelID, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        return bucketsApi.addLabel(labelID,bucketID);
    }

    @Override
    public void deleteLabel(@Nonnull Label label, @Nonnull Bucket bucket) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteLabel(label,bucket);
    }

    @Override
    public void deleteLabel(@Nonnull String labelID, @Nonnull String bucketID) {
        InfluxDBClient client = connectionFactory.getInfluxDBClient();
        BucketsApi bucketsApi = client.getBucketsApi();
        bucketsApi.deleteLabel(labelID, bucketID);
    }
}
