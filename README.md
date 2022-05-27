# InfluxDB Baseline Generator

This project is a standalone application which generates baselines on live and historical data stored in InfluxDB.
The baselines are written back as series to Influx.

## Introduction

Baselines are seasonality based: E.g. a daily baseline is computed by averaging the observed values from the past days at the same hour of the day.

The two core configuration properties for a baselines are *precision* and *seasonality*.

The *precision* defines how many points of the baseline will be computed. For example, given a precision of 30 minutes,
the resulting baseline measurement will consist of two points per hour. Each point defines the value and the standard deviation
for the baseline for a given time interval of 30 minutes.

The *seasonality* defines in what pattern the baselines is expected to reoccur.
A seasonality of one day therefore means that you expect the data to repeat on a daily pattern.
E.g. the value of today at 11 am is expected to correlate with the values of yesterday and the day before at 11 am.
Similarly, a seasonality of seven days can be used for weekly baselines:
This means you expect teh value of monday at 11 am to correlate with the value on the previous mondays at 11 am.


This application is primarily designed for Prometheus-style metrics:
* Counters: Series where the value increases monotonic (e.g. the number of HTTP requests)
* Gauges: Series where the value can go up or down (e.g. the CPU Usage)

For gauges this application will simply use the mean value as baseline.

For counters the increase per second will be baselined. For example given a counter for the HTTP requests,
 the resulting baseline will denote the expected average requests per second in the interval specified by the precision.
 
In addition, it is possible to baseline response times which are derived from counters:
With Prometheus-style metrics, response times are represented using two different counters:
The number of requests and the total time spent processing these requests.
This means that the response time is the ratio of the two: the total time spent divided by the number of requests.
This value can be baselines too, the joining of the series happens within the baseline generator.

In addition to the "counter", "gauge" and "ratio" baselines, there is also a "query" baseline.
Here, a query can be used to specify what data should be used for the baseline calculation.
Based on the data, a mean value will be calculated for the configured precision.
The specified query should use the placeholder `${timeFilter}`, thus, the application can set the query's time window.

## Configuration

The application is a Spring-Boot application. 
It provides a HTTP endpoint which is running by default on port `8080` which is mainly used for providing health information (`/actuator/health`).
The application is configured by placing an `application.yml` file next to the JAR file. 

In the `application.yml` it is first required that you configure the connection to influx:
```
spring:
  influx:
    url: http://localhost:8086
    user: "myuser" # OPTIONAL: username used to connect to influx
    password: "mypw" # OPTIONAL: password used to connect to influx
  
    connect-timeout: 60s # OPTIONAL: timeout to use when connecting to influx
    read-timeout: 60s # OPTIONAL: timeout to use when reading data from influx
    write-timeout: 60s # OPTIONAL: timeout to use when writing data to influx
```

Next you can configure the actual baselining:
```
baselining:

  # When starting up, the service will compute baselines based on historical data
  # This defines how far the service should look into the past
  backfill: 30d
  
  # Commonly data takes some time until it actual gets to the influxDB
  # This property tells the service to wait the given amount of time before updating the baselines.
  # E.g. a delay of 30s means that the baselines for 14:00 to 15:00 will be computed at 15:00:30
  update-delay: 30s
  
  #Baselines for gauge metrics
  gauges:
    - precision: 15m
      seasonality: 1d
      input: telegraf.autogen.system_cpu_usage.gauge
      output: baselines.autogen.system_cpu_usage_daily
      
    - precision: 15m
      seasonality: 7d
      input: telegraf.autogen.system_cpu_usage.gauge
      output: baselines.autogen.system_cpu_usage_weekly
      
  # Baselines for counters (increase per second)
  counters:
    - precision: 15m
      seasonality: 7d
      windows: [28d, 56d]
      input: telegraf.autogen.http_requests_count.value
      output: baselines.autogen.http_request_rate_weekly
      tags: [http_path]
  
  # Baselines for ratio between two counters (e.g. response time)    
  counter-ratios:
    - precision: 15m
      seasonality: 1d
      windows: [15d, 30d]
      input: telegraf.autogen.http_requests_time.counter
      divide-by: telegraf.autogen.http_requests_count.counter
      output: baselines.autogen.http_time_daily
      tags: [http_path]
      
  queries:
   - query: |
       SELECT sum("count") / 60
       FROM ...
       WHERE ${timeFilter} AND ...
       GROUP BY time(60s) fill(0)
     output: baselines.autogen.system_cpu_usage_daily
     precision: 15m
     seasonality: 1d
     windows: [14d]
```

As shown in the examples, each baseline requires you to specify the precision and seasonality which were described above.

In addition, input series are defined in the form `<database>.<retention>.<measurement>.<field>`.
The name of the output baseline is defined as `<database>.<retention>.<measurement>`.

It is possible to specify time windows for each baseline, which have to be multiples of the seasonality.
The time windows define how far the service looks into the past when computing baselines:
E.g. a window of `10d` on a baseline with `seasonality: 1d` means that the baseline values will always only take the past 10 days into account.

The defined output measurement name is actually only used as a prefix, because each window results in a separate measurement.
In the example above, the response time baseline defines `http_time_daily` as output with two windows: `15d` and `30d`.
As result, the service will generate two measurements: `http_time_daily_15d` and `http_time_daily_30d`.
The measurements contain two fields: `value`, which is the baseline and `stddev` which is the standard deviation.

By default, the baseline service will preserve all tags from the input measurement.
When this is not the intended behaviour, it is possible to keep only certain tags (or none).
The values of all other tags will be aggregated together.

For example, if we assume that the `http_requests_count` measurement has two tags (`http_path` and `http_status`),
we can specify `tags: [http_path]` as shown above. This means that the baseline will be generated for each http_path individually,
however the `http_status` will not be used for differentiation.

## SBOM

To generate a software bill of materials (SBOM), execute the gradle task `cyclonedxBom`.
It will save the BOM into the folder build/reports.
