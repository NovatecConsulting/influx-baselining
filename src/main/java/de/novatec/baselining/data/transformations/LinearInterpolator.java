package de.novatec.baselining.data.transformations;

import de.novatec.baselining.data.AbstractTimedPoint;
import de.novatec.baselining.data.DataPoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;

public class LinearInterpolator {

    private TreeSet<DataPoint> pointsSorted;

    public LinearInterpolator(Collection<? extends DataPoint> points){
        pointsSorted = new TreeSet<>(AbstractTimedPoint.TIME_COMPARATOR);
        pointsSorted.addAll(points);
    }

    public double interpolate(long time) {
        DataPoint dummy = new DataPoint(time,0);
        DataPoint lower = pointsSorted.floor(dummy);
        DataPoint upper = pointsSorted.ceiling(dummy);
        if(lower == upper) {
            if(lower == null) {
                return 0;
            } else {
                return lower.getValue();
            }
        } else {
            if(lower == null) {
                return upper.getValue();
            }
            if(upper == null) {
                return lower.getValue();
            }
            double upperWeight = (time - lower.getTime()) / (double)(upper.getTime() - lower.getTime());
            return (1- upperWeight) * lower.getValue() + upperWeight * upper.getValue();
        }
    }
}
