package de.novatec.baselining.spring;

import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.influx.InfluxDbOkHttpClientBuilderProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class BeanConfiguration {

    @Value("${spring.influx.connect-timeout:60s}")
    private Duration connectTimeout;

    @Value("${spring.influx.read-timeout:60s}")
    private Duration readTimeout;

    @Value("${spring.influx.write-timeout:60s}")
    private Duration writeTimeout;

    /**
     * Used by the {@link org.springframework.boot.autoconfigure.influx.InfluxDbAutoConfiguration} to
     * create the {@link org.influxdb.InfluxDB} client.
     *
     * @return HTTP builder which is used for the InfluxDB client.
     */
    @Bean
    public InfluxDbOkHttpClientBuilderProvider influxOkHttpClientBuilderProvider() {
        return () -> {
            OkHttpClient.Builder builder = new OkHttpClient.Builder();

            if (connectTimeout != null) {
                builder.connectTimeout(connectTimeout);
            }
            if (connectTimeout != null) {
                builder.readTimeout(readTimeout);
            }
            if (connectTimeout != null) {
                builder.writeTimeout(writeTimeout);
            }

            return builder;
        };
    }
}
