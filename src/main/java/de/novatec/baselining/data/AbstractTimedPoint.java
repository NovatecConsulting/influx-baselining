package de.novatec.baselining.data;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Comparator;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
public abstract class AbstractTimedPoint {

    public static final Comparator<AbstractTimedPoint> TIME_COMPARATOR = Comparator.comparingLong(AbstractTimedPoint::getTime);

    protected final long time;

    public abstract AbstractTimedPoint shift(long millis);
}
