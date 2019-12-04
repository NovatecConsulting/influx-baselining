package de.novatec.baselining.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.net.URL;
import java.time.Duration;

@Data
@NoArgsConstructor
@ConfigurationProperties("influx")
@Configuration
@Validated
public class InfluxSettings {

    @NotNull
    private URL url;

    private String user;
    private String password;

    @NotNull
    private Duration connectTimeout;

    @NotNull
    private Duration readTimeout;

    @NotNull
    private Duration writeTimeout;
}
