package de.novatec.baselining.datasources;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import de.novatec.baselining.influx.InfluxAccess;
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

    private final InfluxAccess influx;

    private final MeasurementFieldName input;

    private final String query;

    private final List<String> tags;

    private final long lookBackMillis;

    private final long samplePrecisionMillis;

    private final MeasurementName rawOutput;

    public CounterDataSource(InfluxAccess influx, CounterBaselineDefinition settings) {
        this.influx = influx;
        this.input = settings.getInput();
        this.query = "SELECT LAST(" + input.getField() + ") FROM " + input.getFullMeasurementName();
        this.tags = settings.getTags();
        this.lookBackMillis = settings.getLookBack().toMillis();
        this.samplePrecisionMillis = settings.getSamplePrecision().toMillis();
        this.rawOutput = settings.getLoopBackMetric();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> data = influx.queryAggregate(input.getDatabase(), query, start - lookBackMillis, end, samplePrecisionMillis);

        data = Transformations.rateSince(data, start, Duration.ofSeconds(1));

        if (tags != null) {
            data = Aggregations.aggregateByTags(tags, data, (a, b) ->
                    Aggregations.joinInterpolating(a, b, (v1, v2) -> v1 + v2)
            );
        }

        if (rawOutput != null) {
            influx.writePoints(rawOutput.getDatabase(), rawOutput.getMeasurement(), data);
        }
        return Transformations.meanByInterval(data, intervalMillis);
    }

}
