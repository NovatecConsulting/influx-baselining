package de.novatec.baselining.config;

import de.novatec.baselining.config.baselines.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@ConfigurationProperties("baselining")
@Configuration
@Validated
public class BaselineServiceSettings {

    private Duration updateDelay;

    private Duration backfill;

    private List<@Valid QueryBaselineDefinition> queries = new ArrayList<>();
    private List<@Valid GaugeBaselineDefinition> gauges = new ArrayList<>();
    private List<@Valid RateBaselineDefinition> rates = new ArrayList<>();
    private List<@Valid CounterBaselineDefinition> counters = new ArrayList<>();
    private List<@Valid CounterRatioBaselineDefinition> counterRatios = new ArrayList<>();
}
