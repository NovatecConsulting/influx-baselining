package de.novatec.baselining.data;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

@Value
@EqualsAndHashCode(callSuper = true)
public class AggregatePoint extends AbstractTimedPoint {

    double valuesSum;

    double squaredValuesSum;

    long count;

    public double getAvgValue() {
        return valuesSum / count;
    }

    public double getAvgSquaredValue() {
        return squaredValuesSum / count;
    }

    @Builder(toBuilder = true)
    public AggregatePoint(long time, double valuesSum, double squaredValuesSum, long count) {
        super(time);
        this.valuesSum = valuesSum;
        this.squaredValuesSum = squaredValuesSum;
        this.count = count;
    }

    @Override
    public AggregatePoint shift(long millis) {
        return this.toBuilder().time(time + millis).build();
    }

    public AggregatePoint add(AggregatePoint other) {
        return new AggregatePoint(
                this.time,
                this.valuesSum + other.valuesSum,
                this.squaredValuesSum + other.squaredValuesSum,
                this.count + other.count);
    }

    public AggregatePoint add(DataPoint point) {
        double value = point.getValue();
        return new AggregatePoint(
                this.time,
                this.valuesSum + value,
                this.squaredValuesSum + value * value,
                this.count + 1);
    }

    public AggregatePoint minus(AggregatePoint other) {
        return new AggregatePoint(
                this.time,
                this.valuesSum - other.valuesSum,
                this.squaredValuesSum - other.squaredValuesSum,
                this.count - other.count);
    }

    public static AggregatePoint from(DataPoint point) {
        double value = point.getValue();
        return new AggregatePoint(point.getTime(), value, value * value, 1);
    }

    public static AggregatePoint from(long timestamp, Collection<? extends DataPoint> points) {

        long count = points.size();
        double sum = points.stream()
                .mapToDouble(DataPoint::getValue)
                .sum();
        double sumOfSquares = points.stream()
                .mapToDouble(DataPoint::getValue)
                .map(value -> value * value)
                .sum();
        return new AggregatePoint(timestamp, sum, sumOfSquares, count);
    }

    @Override
    public String toString() {
        String timeStr = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format(new Date(time));
        return "(t=" + timeStr + " avg=" + getAvgValue() + " sqAvg=" + getAvgSquaredValue() + " cnt=" + getCount() + ")";
    }
}
