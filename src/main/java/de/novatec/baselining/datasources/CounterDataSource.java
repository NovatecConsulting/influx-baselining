package de.novatec.baselining.datasources;

import de.novatec.baselining.InfluxAccess;
import de.novatec.baselining.config.baselines.CounterBaselineDefinition;
import de.novatec.baselining.config.measurement.MeasurementName;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Aggregations;
import de.novatec.baselining.data.transformations.Transformations;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class CounterDataSource implements BaselineDataSource {


    private InfluxAccess influx;

    private String query;

    private List<String> tags;

    private long lookBackMillis;

    private long samplePrecisionMillis;

    private MeasurementName rawOuput;

    public CounterDataSource(InfluxAccess influx, CounterBaselineDefinition settings) {
        this.influx = influx;
        this.query = "SELECT LAST(" + settings.getInput().getField() + ") FROM " + settings.getInput().getFullMeasurementName();
        this.tags = settings.getTags();
        this.lookBackMillis = settings.getLookBack().toMillis();
        this.samplePrecisionMillis = settings.getSamplePrecision().toMillis();
        this.rawOuput = settings.getLoopBackMetric();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> data = influx.queryAggregate(query, start - lookBackMillis, end, samplePrecisionMillis);

        data = Transformations.rateSince(data, start, Duration.ofSeconds(1));

        if (tags != null) {
            data = Aggregations.aggregateByTags(tags, data, (a, b) ->
                    Aggregations.joinInterpolating(a, b, (v1, v2) -> v1 + v2)
            );
        }

        if (rawOuput != null) {
            influx.writePoints(rawOuput.getDatabase(), rawOuput.getRetention(), rawOuput.getMeasurement(), data);
        }
        return Transformations.meanByInterval(data, intervalMillis);
    }

}
