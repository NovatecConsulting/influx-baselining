package de.novatec.baselining.datasources;

import de.novatec.baselining.influx.InfluxAccess;
import de.novatec.baselining.config.baselines.QueryBaselineDefinition;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Transformations;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class QueryDataSource implements BaselineDataSource {

    private final InfluxAccess influx;

    private final String database;

    private final String queryTemplate;

    public QueryDataSource(InfluxAccess influx, QueryBaselineDefinition settings) {
        this.influx = influx;
        this.database = settings.getOutput().getDatabase();
        this.queryTemplate = settings.getQuery();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> rawPoints = influx.queryTemplate(database, queryTemplate, start, end);

        return Transformations.meanByInterval(rawPoints, intervalMillis);
    }
}
