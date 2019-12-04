package de.novatec.baselining.data;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;


@Value
@EqualsAndHashCode(callSuper = true)
public class DataPoint extends AbstractTimedPoint {

    double value;

    @Builder(toBuilder = true)
    public DataPoint(long time, double value) {
        super(time);
        this.value = value;
    }

    @Override
    public DataPoint shift(long millis) {
        return this.toBuilder().time(time + millis).build();
    }
}
