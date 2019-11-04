package de.novatec.baselining;

import de.novatec.baselining.baselines.BaselineGenerator;
import de.novatec.baselining.config.BaselineServiceSettings;
import de.novatec.baselining.config.baselines.AbstractBaselineDefinition;
import de.novatec.baselining.datasources.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class BaseliningService {

    @Autowired
    BaselineServiceSettings config;

    @Autowired
    InfluxAccess influx;

    private List<BaselineGenerator> baselines;

    private Map<BaselineGenerator, Long> lastUpdatedTimestamp;

    @PostConstruct
    void start() {
        long start = System.currentTimeMillis() - config.getBackfill().toMillis();
        baselines = new ArrayList<>();
        lastUpdatedTimestamp = new HashMap<>();
        baselines.addAll(buildGaugeBaselines());
        baselines.addAll(buildRateBaselines());
        baselines.addAll(buildCounterBaselines());
        baselines.addAll(buildCounterRatioBaselines());
        baselines.forEach(blg -> lastUpdatedTimestamp.put(blg,start));

        new Thread(() -> {
            while(true) {
                updateAll();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }).start();
    }

    private void updateAll() {
        for(BaselineGenerator generator : baselines) {
            long now = System.currentTimeMillis() - config.getUpdateDelay().toMillis() - generator.getMinimumDelayMillis();
            long updateTimestamp = lastUpdatedTimestamp.get(generator);
            try {
                while(generator.getIntervalIndex(now) != generator.getIntervalIndex(updateTimestamp)) {
                    long updateTo = Math.min(now, updateTimestamp + generator.getMaxUpdateIntervalSizeMillis());
                    generator.updateBaselines(updateTimestamp, updateTo);
                    lastUpdatedTimestamp.put(generator, updateTo);
                    updateTimestamp = updateTo;
                }
            } catch(Throwable t) {
                log.error("An error occurred updating the baseline",t);
            }
        }
    }

    private List<BaselineGenerator> buildGaugeBaselines() {
        return config.getGauges().stream()
                .map(definition -> {
                    GaugeDataSource src = new GaugeDataSource(influx, definition);
                    return buildBaselineGenerator(definition, src);
                })
                .collect(Collectors.toList());
    }


    private List<BaselineGenerator> buildCounterBaselines() {
        return config.getCounters().stream()
                .map(definition -> {
                    CounterDataSource src = new CounterDataSource(influx,definition);
                    return buildBaselineGenerator(definition, src);
                })
                .collect(Collectors.toList());
    }

    private List<BaselineGenerator> buildCounterRatioBaselines() {
        return config.getCounterRatios().stream()
                .map(definition -> {
                    CounterRatioDataSource src = new CounterRatioDataSource(influx,definition);
                    return buildBaselineGenerator(definition, src);
                })
                .collect(Collectors.toList());
    }

    private List<BaselineGenerator> buildRateBaselines() {
        return config.getRates().stream()
                .map(definition -> {
                    RateBaselineSource src = new RateBaselineSource(influx,definition);
                    return buildBaselineGenerator(definition, src);
                })
                .collect(Collectors.toList());
    }

    private BaselineGenerator buildBaselineGenerator(AbstractBaselineDefinition definition, BaselineDataSource source) {
        return new BaselineGenerator(influx,source,definition);
    }
}
