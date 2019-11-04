package de.novatec.baselining.baselines;

import de.novatec.baselining.InfluxAccess;
import de.novatec.baselining.InfluxUtils;
import de.novatec.baselining.config.baselines.AbstractBaselineDefinition;
import de.novatec.baselining.config.measurement.MeasurementName;
import de.novatec.baselining.data.AbstractTimedPoint;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.datasources.BaselineDataSource;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
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
     *
     * Therefore it is required, that at least the amount of time returned by this method has passed
     * before invoking {@link #updateBaselines(long, long)}.
     *
     * @return the number of milli seconds to delay the baseline computation
     */
    public long getMinimumDelayMillis() {
        return src.getMinimumDelayMillis();
    }

    /**
     * A suggestion on the maximum number of milliseconds between the start and the end timestamp
     * when invoking {@link #updateBaselines(long, long)}.
     *
     * This minimized therisk of read or write timeouts when generating baselines.
     * @return the number of milliseconds
     */
    public long getMaxUpdateIntervalSizeMillis() {
        return precisionMillis * 100;
    }

    /**
     * Updates the baselines from the given start timestamp to the epoche timestamp.
     * Because baselines actually do operate on intervals,
     * the baseline computation will start with the interval in which the start timestamp lies (inclusive)
     * and end with the interval within which the end timestamp lies (exclusive).
     * @param startMillis the start timestamp since the epoche
     * @param endMillis he start timestamp since the epoche
     */
    public void updateBaselines(long startMillis, long endMillis) {
        long startInterval = getIntervalIndex(startMillis);
        long endInterval = getIntervalIndex(endMillis);
        long seasonIntervalCount = getIntervalIndex(seasonalityMillis);

        Date startDate = new Date(startInterval * precisionMillis);
        Date endDate = new Date(endInterval * precisionMillis);

        log.info("Updating Baselines '{}' from {} to {}", outputPrefix.getFullMeasurementName(),startDate,endDate);

        updateInfinityBaseline(startInterval, endInterval);
        for(long windowSize : windowMillis) {
            updateWindowedBaseline(startInterval + seasonIntervalCount, endInterval +seasonIntervalCount, windowSize);
        }
        log.info("Update finished");
    }

    public long getIntervalIndex(long timestamp) {
        return timestamp / precisionMillis;
    }

    private void updateInfinityBaseline(long startInterval, long endInterval) {
        long seasonIntervalCount = getIntervalIndex(seasonalityMillis);

        long previousRelevant = Math.min(endInterval, startInterval + seasonIntervalCount);
        Map<TagValues, List<AggregatePoint>> previousBaselines = fetchInfinityBaselines(startInterval, previousRelevant);

        Map<TagValues,List<AggregatePoint>> newData = src.fetch(precisionMillis, startInterval, endInterval);

        Set<TagValues> allTags = new HashSet<>();
        allTags.addAll(previousBaselines.keySet());
        allTags.addAll(newData.keySet());

        for(TagValues tags : allTags) {
            List<AggregatePoint> oldBaseline = previousBaselines.get(tags);
            List<AggregatePoint> newPoints = newData.get(tags);

            updateInfinityBaselineSeriesWithNewData(startInterval, endInterval, tags, oldBaseline, newPoints);
        }
    }

    private void updateInfinityBaselineSeriesWithNewData(long startIntervall, long endIntervall, TagValues tags, List<AggregatePoint> oldBaseline, List<AggregatePoint> newPoints) {
        Map<Long,AggregatePoint> intervallToBaselineMap = indexPointsByInterval(oldBaseline);
        Map<Long,AggregatePoint> intervallToDataMap = indexPointsByInterval(newPoints);

        List<AggregatePoint> outputPoints = new ArrayList<>();

        for(long intervall = startIntervall; intervall < endIntervall; intervall++) {

            AggregatePoint previousBaseline = intervallToBaselineMap.get(intervall);
            AggregatePoint newValue = intervallToDataMap.get(intervall);

            AggregatePoint resultBaseline = incrementBaseline(previousBaseline, newValue);

            if(resultBaseline != null) {
                intervallToBaselineMap.put(getIntervalIndex(resultBaseline.getTime()), resultBaseline);
                outputPoints.add(resultBaseline);
            }
        }

        writeBaselinePoints(outputPrefix.getMeasurement()+"_inf",true,tags, outputPoints);
    }

    private void updateWindowedBaseline(long startInterval, long endInterval, long windowDuration) {
        String durationSuffix = "_" + InfluxUtils.prettyPrintDuration(windowDuration);
        long windowIntervallCount = windowDuration/precisionMillis;

        Map<TagValues, List<AggregatePoint>> now = fetchInfinityBaselines(startInterval, endInterval);
        Map<TagValues, List<AggregatePoint>> past = fetchInfinityBaselines(startInterval - windowIntervallCount, endInterval - windowIntervallCount);

        Set<TagValues> allTags = new HashSet<>();
        allTags.addAll(now.keySet());
        allTags.addAll(past.keySet());

        for(TagValues tags : allTags) {
            Map<Long,AggregatePoint> nowValues = indexPointsByInterval(now.get(tags));
            Map<Long,AggregatePoint> pastValues = indexPointsByInterval(past.get(tags));

            List<AggregatePoint> outputPoints = new ArrayList<>();

            for(long intervall = startInterval; intervall < endInterval; intervall++) {

                AggregatePoint previousPoint = pastValues.get(intervall- windowIntervallCount);
                AggregatePoint nowPoint = nowValues.get(intervall);

                AggregatePoint resultBaseline = computeDelta(previousPoint, nowPoint);

                if(resultBaseline != null) {
                    outputPoints.add(resultBaseline);
                }
            }

            writeBaselinePoints(outputPrefix.getMeasurement()+durationSuffix,false,tags, outputPoints);
        }
    }

    private AggregatePoint computeDelta(AggregatePoint firstPoint, AggregatePoint secondPoint) {
        if(firstPoint == null && secondPoint != null) {
            return secondPoint.toBuilder().build();
        } else if(firstPoint != null && secondPoint != null) {
            AggregatePoint diff = secondPoint.minus(firstPoint);
            if(diff.getCount() > 0) {
                return diff;
            }
        }
        return null;

    }

    private void writeBaselinePoints(String measurementName, boolean includeAggregates, TagValues tags, List<AggregatePoint> outputPoints) {
        List<Point> points = outputPoints.stream()
                .map(pt -> toInfluxPoint(pt,measurementName,includeAggregates))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        influx.writePoints(outputPrefix.getDatabase(),outputPrefix.getRetention(),tags.getTags(), points);
    }

    private AggregatePoint incrementBaseline(AggregatePoint previousBaseline, AggregatePoint newSeasonValue) {
        if(newSeasonValue != null) {
            if(previousBaseline == null || previousBaseline.getCount() == 0) {
                return newSeasonValue.shift(seasonalityMillis);
            } else {
                return previousBaseline.add(newSeasonValue).shift(seasonalityMillis);
            }
        } else if(previousBaseline != null) {
            return previousBaseline.shift(seasonalityMillis);
        }
        return null;
    }

    private <PT extends AbstractTimedPoint> Map<Long, PT> indexPointsByInterval(Collection<PT> points) {
        Map<Long,PT> result = new HashMap<>();
        if(points != null) {
            for(PT pt : points) {
                long intervall = getIntervalIndex(pt.getTime());
                if(result.containsKey(intervall)) {
                    throw new IllegalArgumentException("Input point set contains multiple points falling into the same intervall!");
                }
                result.put(intervall, pt);
            }
        }
        return result;
    }

    private Optional<Point> toInfluxPoint(AggregatePoint pt, String measurementName, boolean includeAggregates) {
        if(pt.getCount() == 0) {
            return Optional.empty();
        }
        double value = pt.getAvgValue();
        double stddev = Math.sqrt(Math.max(0, pt.getAvgSquaredValue() - value * value));
        Point.Builder builder = Point
                .measurement(measurementName)
                .time(pt.getTime(), TimeUnit.MILLISECONDS)
                .addField("value", value)
                .addField("stddev", stddev)
                .addField("seasons", pt.getCount());
        if(includeAggregates) {
            builder
                .addField("sum", pt.getValuesSum())
                .addField("sumSq", pt.getSquaredValuesSum());
        }
        return Optional.of(builder.build());
    }

    private Map<TagValues, List<AggregatePoint>> fetchInfinityBaselines(long startIntervall, long endIntervall) {
        long start = startIntervall*precisionMillis;
        long end = endIntervall *precisionMillis;

        String query = "SELECT sum, sumSq, seasons FROM " + outputPrefix.getFullMeasurementName()+"_inf";
        QueryResult result = influx.query(query, start, end);

        Map<TagValues, List<AggregatePoint>> baselines = new HashMap<>();

        if(result.getResults() != null) {
            return result.getResults().stream()
                    .filter(Objects::nonNull)
                    .map(QueryResult.Result::getSeries)
                    .filter(Objects::nonNull)
                    .flatMap(List::stream)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(
                            series -> TagValues.from(series.getTags()),
                            series -> decodeBaselinePoints(series)
                    ));
        }
        return Collections.emptyMap();
    }

    private List<AggregatePoint> decodeBaselinePoints(QueryResult.Series series) {

        int timeIdx = series.getColumns().indexOf("time");
        int seasonsIdx = series.getColumns().indexOf("seasons");
        int sumIdx = series.getColumns().indexOf("sum");
        int sumSqIdx = series.getColumns().indexOf("sumSq");

        List<AggregatePoint> baselinePoints = new ArrayList<>();

        for(List<Object> values : series.getValues()) {

            Object time = values.get(timeIdx);
            Object seasons = values.get(seasonsIdx);
            Object sum = values.get(sumIdx);
            Object sumSq = values.get(sumSqIdx);

            if(time instanceof  Number && seasons instanceof Number && sum instanceof Number && sumSq instanceof Number) {
                AggregatePoint pt = AggregatePoint.builder()
                        .time(((Number)time).longValue())
                        .count(((Number)seasons).longValue())
                        .valuesSum(((Number)sum).doubleValue())
                        .squaredValuesSum(((Number)sumSq).doubleValue())
                        .build();
                baselinePoints.add(pt);
            }
        }
        return baselinePoints;
    }

}
