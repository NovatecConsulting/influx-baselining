package de.novatec.baselining.datasources;

import de.novatec.baselining.InfluxAccess;
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


    private InfluxAccess influx;

    private String numeratorQuery;
    private String denominatorQuery;

    private List<String> tags;

    private long lookBackMillis;

    private long samplePrecisionMillis;

    private MeasurementName rawOuput;

    public CounterRatioDataSource(InfluxAccess influx, CounterRatioBaselineDefinition settings) {
        this.influx = influx;
        this.numeratorQuery = "SELECT LAST(" + settings.getInput().getField() + ") FROM " + settings.getInput().getFullMeasurementName();
        this.denominatorQuery = "SELECT LAST(" + settings.getDivideBy().getField() + ") FROM " + settings.getDivideBy().getFullMeasurementName();
        this.tags = settings.getTags();
        this.lookBackMillis = settings.getLookBack().toMillis();
        this.samplePrecisionMillis = settings.getSamplePrecision().toMillis();
        this.rawOuput = settings.getLoopBackMetric();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {

        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> numerators = influx.queryAggregate(numeratorQuery, start - lookBackMillis, end, samplePrecisionMillis);
        Map<TagValues, List<DataPoint>> denominators = influx.queryAggregate(denominatorQuery, start - lookBackMillis, end, samplePrecisionMillis);


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
            influx.writePoints(rawOuput.getDatabase(), rawOuput.getRetention(), rawOuput.getMeasurement(), averages);
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
