package de.novatec.baselining.datasources;

import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.TagValues;

import java.util.List;
import java.util.Map;

/**
 * Queries influx and returns the aggregated data on which the baselining is performed.
 */
public interface BaselineDataSource {

    /**
     * Provides the data for buildign a baseline over a specified time range.
     * <p>
     * Baselines operator on intervals, which are specified by the precision setting.
     * It is required, that this method returns at most one point per interval.
     *
     * @param intervalMillis the number of milliseconds within one interval
     * @param startInterval  the index of the interval to start with (inclusive), meaning the query should start at intervalMillis*startInterval
     * @param endInterval    the index of the interval to end with (exclusive), meaning the query should end at (endInterval-1)*startInterval
     * @return the aggregates which are used to update the baseline
     */
    Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval);

    default long getMinimumDelayMillis() {
        return 0;
    }
}
