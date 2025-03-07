package de.novatec.baselining.datasources;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import de.novatec.baselining.influx.InfluxAccess;
import de.novatec.baselining.config.baselines.OutlierRemovalSettings;
import de.novatec.baselining.config.baselines.RateBaselineDefinition;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Aggregations;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.*;
import java.util.stream.Collectors;

/** Is this even used? */
@Slf4j
public class RateBaselineSource implements BaselineDataSource {

    private final InfluxAccess influx;

    private final MeasurementFieldName input;

    private final String query;

    private final List<String> tags;

    private final OutlierRemovalSettings outlierRemovalConfig;

    public RateBaselineSource(InfluxAccess influx, RateBaselineDefinition settings) {
        this.influx = influx;
        this.input = settings.getInput();
        this.query = "SELECT " + input.getField() + " FROM " + input.getFullMeasurementName();
        this.tags = settings.getTags();
        this.outlierRemovalConfig = settings.getOutliers();
    }

    @Override
    public long getMinimumDelayMillis() {
        return outlierRemovalConfig.getWindow().toMillis() / 2;
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        Map<TagValues, List<DataPoint>> filteredPoints = fetchFilteredData(intervalMillis, startInterval, endInterval);

        double intervallToHours = 60 * 60 * 1000.0 / intervalMillis;

        Map<TagValues, List<AggregatePoint>> result = new HashMap<>();
        filteredPoints.forEach((tagValues, values) -> {
            List<AggregatePoint> aggregated = Aggregations
                    .byIntervall(values, intervalMillis, AggregatePoint::from).stream()
                    .map(pt -> new AggregatePoint(pt.getTime(),
                            pt.getValuesSum() * intervallToHours,
                            pt.getValuesSum() * intervallToHours * pt.getValuesSum() * intervallToHours,
                            1))
                    .map(pt -> pt.shift(-intervalMillis / 2))
                    .collect(Collectors.toList());
            result.put(tagValues, aggregated);
        });


        return result;
    }

    private Map<TagValues, List<DataPoint>> fetchFilteredData(long intervallMillis, long startIntervall, long endIntervall) {
        long start = startIntervall * intervallMillis;
        long end = endIntervall * intervallMillis;

        long outlierWindowSize = outlierRemovalConfig.getWindow().toMillis();
        long startWithHalo = start - outlierWindowSize / 2;
        long endWithHalo = end + outlierWindowSize / 2;

        Map<TagValues, List<DataPoint>> rawPoints = influx.querySingleField(input.getDatabase(), query, startWithHalo, endWithHalo);
        if (tags != null) {
            rawPoints = Aggregations.aggregateByTags(tags, rawPoints, (a, b) -> {
                ArrayList<DataPoint> combined = new ArrayList<>(a);
                combined.addAll(b);
                return combined;
            });
        }


        return rawPoints.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        points -> filterOutliers(points.getValue(), intervallMillis, startIntervall, endIntervall)));
    }

    private List<DataPoint> filterOutliers(Collection<DataPoint> data, long intervallMillis, long startIntervall, long endIntervall) {

        List<DataPoint> result = new ArrayList<>();

        TreeMap<Long, DataPoint> pointsSorted = new TreeMap<>();
        data.forEach(pt -> pointsSorted.put(pt.getTime(), pt));
        for (long interval = startIntervall; interval < endIntervall; interval++) {

            long intervalStart = interval * intervallMillis;
            long intervalEnd = (interval + 1) * intervallMillis;
            long intervalCenter = interval * intervallMillis;

            long windowStart = intervalCenter - outlierRemovalConfig.getWindow().toMillis() / 2;
            long windowEnd = intervalCenter + outlierRemovalConfig.getWindow().toMillis() / 2;

            double percentileValue = Double.MAX_VALUE;

            Collection<DataPoint> windowPoints = pointsSorted.subMap(windowStart, true, windowEnd, false).values();
            if (windowPoints.size() >= outlierRemovalConfig.getMinPointCount()) {
                double[] valuesArray = windowPoints.stream().mapToDouble(DataPoint::getValue).toArray();
                Percentile p = new Percentile(outlierRemovalConfig.getPercentile() * 100);
                p.setData(valuesArray);
                percentileValue = p.evaluate();
            }

            double percentileLimit = percentileValue;
            pointsSorted.subMap(intervalStart, true, intervalEnd, false)
                    .values().stream()
                    .filter(pt -> pt.getValue() <= percentileLimit)
                    .forEach(result::add);
        }
        return result;
    }
}
