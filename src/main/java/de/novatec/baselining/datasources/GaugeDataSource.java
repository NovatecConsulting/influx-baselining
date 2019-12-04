package de.novatec.baselining.datasources;

import de.novatec.baselining.InfluxAccess;
import de.novatec.baselining.config.baselines.GaugeBaselineDefinition;
import de.novatec.baselining.config.measurement.MeasurementName;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Aggregations;
import de.novatec.baselining.data.transformations.Transformations;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class GaugeDataSource implements BaselineDataSource {


    private InfluxAccess influx;

    private String query;

    private List<String> tags;

    private long samplePrecisionMillis;

    private MeasurementName rawOuput;

    public GaugeDataSource(InfluxAccess influx, GaugeBaselineDefinition settings) {
        this.influx = influx;
        this.query = "SELECT MEAN(" + settings.getInput().getField() + ") FROM " + settings.getInput().getFullMeasurementName();
        this.tags = settings.getTags();
        this.samplePrecisionMillis = settings.getSamplePrecision().toMillis();
        this.rawOuput = settings.getLoopBackMetric();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> rawPoints = influx.queryAggregate(query, start, end, samplePrecisionMillis);

        if (tags != null) {
            rawPoints = Aggregations.aggregateByTags(tags, rawPoints, (a, b) -> {
                ArrayList<DataPoint> combined = new ArrayList<>(a);
                combined.addAll(b);
                return combined;
            });
        }


        if (rawOuput != null) {
            influx.writePoints(rawOuput.getDatabase(), rawOuput.getRetention(), rawOuput.getMeasurement(), rawPoints);
        }

        return Transformations.meanByInterval(rawPoints, intervalMillis);
    }
}
