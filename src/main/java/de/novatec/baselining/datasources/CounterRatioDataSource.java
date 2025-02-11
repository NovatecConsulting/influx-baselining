package de.novatec.baselining.datasources;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import de.novatec.baselining.influx.InfluxAccess;
import de.novatec.baselining.config.baselines.CounterRatioBaselineDefinition;
import de.novatec.baselining.config.measurement.MeasurementName;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Aggregations;
import de.novatec.baselining.data.transformations.Transformations;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CounterRatioDataSource implements BaselineDataSource {

    private final InfluxAccess influx;

    private final MeasurementFieldName input;

    private final MeasurementFieldName divideBy;

    private final String numeratorQuery;

    private final String denominatorQuery;

    private final List<String> tags;

    private final long lookBackMillis;

    private final long samplePrecisionMillis;

    private final MeasurementName rawOuput;

    public CounterRatioDataSource(InfluxAccess influx, CounterRatioBaselineDefinition settings) {
        this.influx = influx;
        this.input = settings.getInput();
        this.divideBy = settings.getDivideBy();
        this.numeratorQuery = "SELECT LAST(" + input.getField() + ") FROM " + input.getFullMeasurementName();
        this.denominatorQuery = "SELECT LAST(" + divideBy.getField() + ") FROM " + divideBy.getFullMeasurementName();
        this.tags = settings.getTags();
        this.lookBackMillis = settings.getLookBack().toMillis();
        this.samplePrecisionMillis = settings.getSamplePrecision().toMillis();
        this.rawOuput = settings.getLoopBackMetric();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {

        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> numerators = influx.queryAggregate(input.getDatabase(), numeratorQuery, start - lookBackMillis, end, samplePrecisionMillis);
        Map<TagValues, List<DataPoint>> denominators = influx.queryAggregate(divideBy.getDatabase(), denominatorQuery, start - lookBackMillis, end, samplePrecisionMillis);


        if (tags != null) {
            numerators = Aggregations.aggregateByTags(tags, numerators, (a, b) ->
                    Aggregations.joinInterpolating(a, b, (v1, v2) -> v1 + v2)
            );
            denominators = Aggregations.aggregateByTags(tags, denominators, (a, b) ->
                    Aggregations.joinInterpolating(a, b, (v1, v2) -> v1 + v2)
            );
        }

        Map<TagValues, List<DataPoint>> averages = divideCounters(start, numerators, denominators);

        if (rawOuput != null) {
            influx.writePoints(rawOuput.getDatabase(), rawOuput.getMeasurement(), averages);
        }

        return Transformations.meanByInterval(averages, intervalMillis);
    }

    private Map<TagValues, List<DataPoint>> divideCounters(long start, Map<TagValues, List<DataPoint>> numerators, Map<TagValues, List<DataPoint>> denominators) {
        Map<TagValues, List<DataPoint>> averages = new HashMap<>();
        for (TagValues tags : numerators.keySet()) {
            if (denominators.containsKey(tags)) {
                List<DataPoint> joined = Aggregations.joinInterpolating(
                        Transformations.rate(numerators.get(tags), Duration.ofHours(1)),
                        Transformations.rate(denominators.get(tags), Duration.ofHours(1)),
                        (num, denom) -> {
                            if (denom > 0) {
                                return num / denom;
                            } else {
                                return null;
                            }
                        }
                ).stream()
                        .filter(pt -> pt.getTime() >= start)
                        .collect(Collectors.toList());
                averages.put(tags, joined);
            }
        }
        return averages;
    }
}
