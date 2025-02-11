package de.novatec.baselining.baselines;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.InfluxQLQueryResult;
import de.novatec.baselining.influx.InfluxAccess;
import de.novatec.baselining.influx.InfluxUtils;
import de.novatec.baselining.config.baselines.AbstractBaselineDefinition;
import de.novatec.baselining.config.measurement.MeasurementName;
import de.novatec.baselining.data.AbstractTimedPoint;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.datasources.BaselineDataSource;
import lombok.extern.slf4j.Slf4j;
import com.influxdb.client.write.Point;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Computes and writes baselines to influx based on a given {@link BaselineDataSource}.
 */
@Slf4j
public class BaselineGenerator {

    private InfluxAccess influx;

    private BaselineDataSource src;

    private long precisionMillis;

    private long seasonalityMillis;

    private List<Long> windowMillis;

    private MeasurementName outputPrefix;

    public BaselineGenerator(InfluxAccess influx, BaselineDataSource src, AbstractBaselineDefinition definition) {
        this.influx = influx;
        this.src = src;
        this.precisionMillis = definition.getPrecision().toMillis();
        this.seasonalityMillis = definition.getSeasonality().toMillis();
        this.outputPrefix = definition.getOutput();
        this.windowMillis = definition.getWindowsWithDefault().stream()
                .map(Duration::toMillis)
                .collect(Collectors.toList());
    }

    /**
     * Some BaselineSources need to know data in the future for computing baselines at a given point in time.
     * For example, in order to perform outlier filtering.
     * <p>
     * Therefore it is required, that at least the amount of time returned by this method has passed
     * before invoking {@link #updateBaselines(long, long)}.
     *
     * @return the number of milliseconds to delay the baseline computation
     */
    public long getMinimumDelayMillis() {
        return src.getMinimumDelayMillis();
    }

    /**
     * A suggestion on the maximum number of milliseconds between the start and the end timestamp
     * when invoking {@link #updateBaselines(long, long)}.
     * <p>
     * This minimizes risk of read or write timeouts when generating baselines.
     *
     * @return the number of milliseconds
     */
    public long getMaxUpdateIntervalSizeMillis() {
        return precisionMillis * 100;
    }

    /**
     * Updates the baselines from the given start timestamp to the epoch timestamp.
     * Because baselines actually do operate on intervals,
     * the baseline computation will start with the interval in which the start timestamp lies (inclusive)
     * and end with the interval within which the end timestamp lies (exclusive).
     *
     * @param startMillis the start timestamp since the epoch
     * @param endMillis   the start timestamp since the epoch
     */
    public void updateBaselines(long startMillis, long endMillis) {
        long startInterval = getIntervalIndex(startMillis);
        long endInterval = getIntervalIndex(endMillis);
        long seasonIntervalCount = getIntervalIndex(seasonalityMillis);

        Date startDate = new Date(startInterval * precisionMillis);
        Date endDate = new Date(endInterval * precisionMillis);

        log.info("Updating Baselines '{}' from {} to {}", outputPrefix.getFullMeasurementName(), startDate, endDate);

        updateInfinityBaseline(startInterval, endInterval);
        for (long windowSize : windowMillis) {
            updateWindowedBaseline(startInterval + seasonIntervalCount, endInterval + seasonIntervalCount, windowSize);
        }
        log.info("Update finished");
    }

    public long getIntervalIndex(long timestamp) {
        return timestamp / precisionMillis;
    }

    private void updateInfinityBaseline(long startInterval, long endInterval) {
        long seasonIntervalCount = getIntervalIndex(seasonalityMillis);

        long previousRelevant = Math.min(endInterval, startInterval + seasonIntervalCount);
        Map<TagValues, List<AggregatePoint>> previousBaselines = fetchInfinityBaselines(outputPrefix.getDatabase(), startInterval, previousRelevant);

        Map<TagValues, List<AggregatePoint>> newData = src.fetch(precisionMillis, startInterval, endInterval);

        Set<TagValues> allTags = new HashSet<>();
        allTags.addAll(previousBaselines.keySet());
        allTags.addAll(newData.keySet());

        List<Point> baselinePoints = new LinkedList<>();

        for (TagValues tags : allTags) {
            List<AggregatePoint> oldBaseline = previousBaselines.get(tags);
            List<AggregatePoint> newPoints = newData.get(tags);

            List<Point> points = generateInfinityBaselineSeriesWithNewData(startInterval, endInterval, tags, oldBaseline, newPoints);
            baselinePoints.addAll(points);
        }

        influx.writePoints(outputPrefix.getDatabase(), Collections.emptyMap(), baselinePoints);
    }

    private List<Point> generateInfinityBaselineSeriesWithNewData(long startInterval, long endInterval, TagValues tags, List<AggregatePoint> oldBaseline, List<AggregatePoint> newPoints) {
        Map<Long, AggregatePoint> intervallToBaselineMap = indexPointsByInterval(oldBaseline);
        Map<Long, AggregatePoint> intervallToDataMap = indexPointsByInterval(newPoints);

        List<AggregatePoint> outputPoints = new ArrayList<>();

        for (long interval = startInterval; interval < endInterval; interval++) {

            AggregatePoint previousBaseline = intervallToBaselineMap.get(interval);
            AggregatePoint newValue = intervallToDataMap.get(interval);

            AggregatePoint resultBaseline = incrementBaseline(previousBaseline, newValue);

            if (resultBaseline != null) {
                intervallToBaselineMap.put(getIntervalIndex(resultBaseline.getTime()), resultBaseline);
                outputPoints.add(resultBaseline);
            }
        }

        return generateBaselinePoints(outputPrefix.getMeasurement() + "_inf", true, tags, outputPoints);
    }

    private void updateWindowedBaseline(long startInterval, long endInterval, long windowDuration) {
        String durationSuffix = "_" + InfluxUtils.prettyPrintDuration(windowDuration);
        long windowIntervalCount = windowDuration / precisionMillis;

        Map<TagValues, List<AggregatePoint>> now = fetchInfinityBaselines(outputPrefix.getDatabase(), startInterval, endInterval);
        Map<TagValues, List<AggregatePoint>> past = fetchInfinityBaselines(outputPrefix.getDatabase(), startInterval - windowIntervalCount, endInterval - windowIntervalCount);

        Set<TagValues> allTags = new HashSet<>();
        allTags.addAll(now.keySet());
        allTags.addAll(past.keySet());

        List<Point> baselinePoints = new LinkedList<>();

        for (TagValues tags : allTags) {
            Map<Long, AggregatePoint> nowValues = indexPointsByInterval(now.get(tags));
            Map<Long, AggregatePoint> pastValues = indexPointsByInterval(past.get(tags));

            List<AggregatePoint> outputPoints = new ArrayList<>();

            for (long intervall = startInterval; intervall < endInterval; intervall++) {

                AggregatePoint previousPoint = pastValues.get(intervall - windowIntervalCount);
                AggregatePoint nowPoint = nowValues.get(intervall);

                AggregatePoint resultBaseline = computeDelta(previousPoint, nowPoint);

                if (resultBaseline != null) {
                    outputPoints.add(resultBaseline);
                }
            }

            List<Point> points = generateBaselinePoints(outputPrefix.getMeasurement() + durationSuffix, false, tags, outputPoints);
            baselinePoints.addAll(points);
        }

        influx.writePoints(outputPrefix.getDatabase(), Collections.emptyMap(), baselinePoints);
    }

    private AggregatePoint computeDelta(AggregatePoint firstPoint, AggregatePoint secondPoint) {
        if (firstPoint == null && secondPoint != null) {
            return secondPoint.toBuilder().build();
        } else if (firstPoint != null && secondPoint != null) {
            AggregatePoint diff = secondPoint.minus(firstPoint);
            if (diff.getCount() > 0) {
                return diff;
            }
        }
        return null;

    }

    private List<Point> generateBaselinePoints(String measurementName, boolean includeAggregates, TagValues tags, List<AggregatePoint> outputPoints) {
        List<Point> points = outputPoints.stream()
                .map(pt -> toInfluxPoint(pt, measurementName, tags.getTags(), includeAggregates))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        return points;
    }

    private AggregatePoint incrementBaseline(AggregatePoint previousBaseline, AggregatePoint newSeasonValue) {
        if (newSeasonValue != null) {
            if (previousBaseline == null || previousBaseline.getCount() == 0) {
                return newSeasonValue.shift(seasonalityMillis);
            } else {
                return previousBaseline.add(newSeasonValue).shift(seasonalityMillis);
            }
        } else if (previousBaseline != null) {
            return previousBaseline.shift(seasonalityMillis);
        }
        return null;
    }

    private <PT extends AbstractTimedPoint> Map<Long, PT> indexPointsByInterval(Collection<PT> points) {
        Map<Long, PT> result = new HashMap<>();
        if (points != null) {
            for (PT pt : points) {
                long interval = getIntervalIndex(pt.getTime());
                if (result.containsKey(interval)) {
                    throw new IllegalArgumentException("Input point set contains multiple points falling into the same interval!");
                }
                result.put(interval, pt);
            }
        }
        return result;
    }

    private Optional<Point> toInfluxPoint(AggregatePoint pt, String measurementName, Map<String, String> tags, boolean includeAggregates) {
        if (pt.getCount() == 0) {
            return Optional.empty();
        }
        double value = pt.getAvgValue();
        double stddev = Math.sqrt(Math.max(0, pt.getAvgSquaredValue() - value * value));

        Point point = new Point(measurementName);
        point.time(pt.getTime(), WritePrecision.MS)
            .addField("value", value)
            .addField("stddev", stddev)
            .addField("seasons", pt.getCount())
            .addTags(tags);

        if (includeAggregates) {
            point.addField("sum", pt.getValuesSum())
                .addField("sumSq", pt.getSquaredValuesSum());
        }
        return Optional.of(point);
    }

    private Map<TagValues, List<AggregatePoint>> fetchInfinityBaselines(String database, long startIntervall, long endIntervall) {
        long start = startIntervall * precisionMillis;
        long end = endIntervall * precisionMillis;

        String selectFromQuery = "SELECT sum, sumSq, seasons FROM " + outputPrefix.getFullMeasurementName() + "_inf";
        InfluxQLQueryResult result = influx.query(database, selectFromQuery, start, end);

        Map<TagValues, List<AggregatePoint>> baselines = result.getResults().stream()
                .filter(Objects::nonNull)
                .map(InfluxQLQueryResult.Result::getSeries)
                .flatMap(List::stream)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        series -> TagValues.from(series.getTags()),
                        series -> decodeBaselinePoints(series)
                ));
        return baselines;
    }

    private List<AggregatePoint> decodeBaselinePoints(InfluxQLQueryResult.Series series) {
        int timeIdx = series.getColumns().get("time");
        int seasonsIdx = series.getColumns().get("seasons");
        int sumIdx = series.getColumns().get("sum");
        int sumSqIdx = series.getColumns().get("sumSq");

        List<AggregatePoint> baselinePoints = new ArrayList<>();

        for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
            Object[] values = record.getValues();
            Object time = values[timeIdx];
            Object seasons = values[seasonsIdx];
            Object sum = values[sumIdx];
            Object sumSq = values[sumSqIdx];

            if (time instanceof Number && seasons instanceof Number && sum instanceof Number && sumSq instanceof Number) {
                AggregatePoint pt = AggregatePoint.builder()
                        .time(((Number) time).longValue())
                        .count(((Number) seasons).longValue())
                        .valuesSum(((Number) sum).doubleValue())
                        .squaredValuesSum(((Number) sumSq).doubleValue())
                        .build();
                baselinePoints.add(pt);
            }

            try {
                // convert ns to ms
                long timeValue = Long.parseLong(time.toString()) / 1000 / 1000;
                long countValue = Long.parseLong(seasons.toString());
                double sumValue = Double.parseDouble(sum.toString());
                double sumSqValue = Double.parseDouble(sumSq.toString());
                AggregatePoint pt = AggregatePoint.builder()
                        .time(timeValue)
                        .count(countValue)
                        .valuesSum(sumValue)
                        .squaredValuesSum(sumSqValue)
                        .build();
                baselinePoints.add(pt);
            } catch (NumberFormatException e) {
                // Ignore value
            }
        }
        return baselinePoints;
    }

}
