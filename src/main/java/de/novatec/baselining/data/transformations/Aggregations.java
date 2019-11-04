package de.novatec.baselining.data.transformations;

import de.novatec.baselining.data.AbstractTimedPoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Aggregations {

    public static <Pt extends AbstractTimedPoint> Map<TagValues, List<Pt>> aggregateByTags(Collection<String> tagsToKeep, Map<TagValues, List<Pt>> data, BiFunction<List<Pt>, List<Pt>, List<Pt>> aggregation) {
        Map<TagValues, List<Pt>> result = new HashMap<>();
        data.forEach((tags,values) -> {
            TagValues newTags = tags.keepOnly(tagsToKeep);
            List<Pt> previousValues = result.get(newTags);
            if(previousValues == null) {
                result.put(newTags, values);
            } else {
                result.put(newTags, aggregation.apply(previousValues,values));
            }
        });
        return result;
    }

    public static <R,PT extends AbstractTimedPoint> List<R> byIntervall(Collection<? extends PT> points, long intervallMillis, BiFunction<Long,? super List<PT>,? extends R> aggregation) {

        PriorityQueue<PT> pq = new PriorityQueue<>(AbstractTimedPoint.TIME_COMPARATOR);
        pq.addAll(points);

        long currentIntervall = -1;
        List<PT> pointsInIntervall = new ArrayList<>();

        List<R> results = new ArrayList<>();

        while(!pq.isEmpty()) {
            PT point = pq.poll();
            long pointIntervall = point.getTime() / intervallMillis;
            if(pointIntervall != currentIntervall) {
                if(pointsInIntervall.size() > 0) {
                    long time = currentIntervall * intervallMillis;
                    results.add(aggregation.apply(time,pointsInIntervall));
                    pointsInIntervall= new ArrayList<>();
                }
                currentIntervall = pointIntervall;
            }
            pointsInIntervall.add(point);
        }
        if(pointsInIntervall.size() > 0) {
            long time = currentIntervall * intervallMillis;
            results.add(aggregation.apply(time,pointsInIntervall));
        }
        return results;
    }

    public static List<DataPoint> joinInterpolating(
            Collection<? extends DataPoint> left,
            Collection<? extends DataPoint> right,
            BiFunction<Double,Double,Double> aggregation) {

        LinearInterpolator leftInterpolation = new LinearInterpolator(left);
        LinearInterpolator rightInterpolation = new LinearInterpolator(right);

        return joinByTime(left,right,
                (ptL,ptR) -> Optional.ofNullable(aggregation.apply(ptL.getValue(), ptR.getValue()))
                        .map(value -> new DataPoint(ptL.getTime(), value))
                        .orElse(null),
                (ptL) -> Optional.ofNullable(aggregation.apply(ptL.getValue(), rightInterpolation.interpolate(ptL.getTime())))
                        .map(value -> new DataPoint(ptL.getTime(), value))
                        .orElse(null),
                (ptR) -> Optional.ofNullable(aggregation.apply(leftInterpolation.interpolate(ptR.getTime()), ptR.getValue()))
                        .map(value -> new DataPoint(ptR.getTime(), value))
                        .orElse(null)
                );

    }

    public static <R,PTL extends AbstractTimedPoint,PTR extends AbstractTimedPoint> List<R> joinByTime(
            Collection<? extends PTL> left,
            Collection<? extends PTR> right,
            BiFunction<? super PTL,? super PTR,? extends R> joinFunction,
            Function<? super PTL,? extends R> leftOuterFunction,
            Function<? super PTR,? extends R> rightOuterFunction) {

        PriorityQueue<PTL> leftPq = new PriorityQueue<>(AbstractTimedPoint.TIME_COMPARATOR);
        leftPq.addAll(left);

        PriorityQueue<PTR> rightPq = new PriorityQueue<>(AbstractTimedPoint.TIME_COMPARATOR);
        rightPq.addAll(right);

        List<R> results = new ArrayList<>();

        while(!leftPq.isEmpty() && ! rightPq.isEmpty()) {
            long timeLeft = leftPq.peek().getTime();
            long timeRight = rightPq.peek().getTime();
            R result = null;
            if(timeLeft == timeRight) {
                result = joinFunction.apply(leftPq.poll(), rightPq.poll());
            } else if (timeLeft < timeRight) {
                PTL point = leftPq.poll();
                if(leftOuterFunction != null) {
                    result = leftOuterFunction.apply(point);
                }
            } else {
                PTR point = rightPq.poll();
                if(rightOuterFunction != null) {
                    result = rightOuterFunction.apply(point);
                }
            }
            if(result != null) {
                results.add(result);
            }
        }
        if(leftOuterFunction != null) {
            leftPq.stream()
                    .map(leftOuterFunction)
                    .filter(Objects::nonNull)
                    .forEach(results::add);
        }
        if(rightOuterFunction != null) {
            rightPq.stream()
                    .map(rightOuterFunction)
                    .filter(Objects::nonNull)
                    .forEach(results::add);
        }
        return results;
    }

    public static <R extends AbstractTimedPoint,PTL extends R,PTR extends R> List<R> outerJoinByTime(
            Collection<? extends PTL> left,
            Collection<? extends PTR> right,
            BiFunction<? super PTL,? super PTR,? extends R> joinFunction) {
        return joinByTime(left,right,joinFunction, Function.identity(), Function.identity());
    }
}
