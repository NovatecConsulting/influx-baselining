package de.novatec.baselining.data.transformations;

import de.novatec.baselining.data.AbstractTimedPoint;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Transformations {

    /**
     * Returns the difference betweens successive points
     *
     * @param points
     */
    public static List<DataPoint> rate(Collection<? extends DataPoint> points, Duration unit) {
        double unitMillis = unit.toMillis();
        return combineNeighbors(points, (first, second) -> {
            double diff = second.getValue() - first.getValue();
            double timeDiff = second.getTime() - first.getTime();
            if (timeDiff > 0) {
                return new DataPoint(second.getTime(), diff / timeDiff * unitMillis);
            } else {
                return null;
            }
        });
    }

    public static List<DataPoint> rateSince(Collection<? extends DataPoint> points, long sinceTimestamp, Duration unit) {
        return Transformations.rate(points, Duration.ofSeconds(1)).stream()
                .filter(pt -> pt.getTime() >= sinceTimestamp)
                .collect(Collectors.toList());
    }


    public static <K> Map<K, List<DataPoint>> rateSince(Map<K, ? extends Collection<? extends DataPoint>> data, long sinceTimestamp, Duration unit) {
        return mapValues(data, pts -> rateSince(pts, sinceTimestamp, unit));
    }

    public static <K, I, R> Map<K, R> mapValues(Map<K, I> input, Function<? super I, ? extends R> transformation) {
        Map<K, R> result = new HashMap<>();
        input.forEach((key, value) -> result.put(key, transformation.apply(value)));
        return result;
    }

    public static <PT extends AbstractTimedPoint, R> List<R> combineNeighbors(Collection<PT> points, BiFunction<PT, PT, R> combiner) {
        ArrayList<PT> sorted = new ArrayList<>(points);
        sorted.sort(AbstractTimedPoint.TIME_COMPARATOR);
        ArrayList<R> result = new ArrayList<>();
        for (int i = 1; i < sorted.size(); i++) {
            PT previous = sorted.get(i - 1);
            PT current = sorted.get(i);
            R resultValue = combiner.apply(previous, current);
            if (resultValue != null) {
                result.add(resultValue);
            }
        }
        return result;
    }

    public static Map<TagValues, List<AggregatePoint>> meanByInterval(Map<TagValues, List<DataPoint>> data, long intervalMillis) {
        return mapValues(data, points ->
                Aggregations
                        .byIntervall(points, intervalMillis, AggregatePoint::from)
                        .stream()
                        .map(pt -> new AggregatePoint(pt.getTime(), pt.getAvgValue(), pt.getAvgSquaredValue(), 1))
                        .collect(Collectors.toList())
        );
    }
}
