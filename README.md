## Spring Data InfluxDB

This project is completed with reference to [miwurster/spring-data-influxdb: Spring Data InfluxDB (github.com)](https://github.com/miwurster/spring-data-influxdb). 

The purpose of this project is to simplify the operation of `influxdb:2.X` in spring boot

The influxdbTemplate in the project simply encapsulates the API provided in influxdb client Java: 6.0.0.

## Install

```shell
mvn client install
```

## Artifacts

### Maven

```xml
<dependency>
    <groupId>com.github.sheledon</groupId>
    <artifactId>spring-data-influxdb</artifactId>
    <version>2.2.0</version>
</dependency>
```

## Usage (Spring Boot)

- Following properties can be used in your `application.yml`:

  ```yaml
  spring:
    influxdb:
      url: http://localhost:8086	
      token: fdsafdafds
      org: ks
      bucket: test
  ```

  For detailed configuration, please refer to `org.springframework.data.influxdb.InfluxDBProperties`

- Create `InfluxDBConnectionFactory` and `InfluxDBTemplate` beans:

  ```java
  @Configuration
  @EnableConfigurationProperties(InfluxDBProperties.class)
  public class InfluxDbConfig {
      @Bean
      public InfluxDBConnectionFactory connectionFactory(final InfluxDBProperties properties) {
          return new InfluxDBConnectionFactory(properties);
      }
      @Bean
      public InfluxDBTemplate<Point> influxDBTemplate(final InfluxDBConnectionFactory influxDBConnectionFactory){
          return new InfluxDBTemplate<>(influxDBConnectionFactory);
      }
  }
  ```

- Use `InfluxDBTemplate` to write measurement

  ```java
  @SpringBootTest
  class DemoApplicationTests {
      @Autowired
      private InfluxDBTemplate influxDBTemplate;
      @Test
      void contextLoads() {
          Temperature tem = Temperature.builder()
                  .location("abc")
                  .value(23.0)
                  .time(Instant.now())
                  .build();
          influxDBTemplate.writeMeasurement(WritePrecision.MS,tem);
      }
      
      @Builder
      @Measurement(name = "temperature")
      public class Temperature {
          @Column(tag = true)
          String location;
  
          @Column
          Double value;
  
          @Column(timestamp = true)
          Instant time;
      }
  }
  ```

  

