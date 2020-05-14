package de.novatec.baselining.datasources;

import de.novatec.baselining.InfluxAccess;
import de.novatec.baselining.config.baselines.GaugeBaselineDefinition;
import de.novatec.baselining.config.baselines.QueryBaselineDefinition;
import de.novatec.baselining.config.measurement.MeasurementName;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Aggregations;
import de.novatec.baselining.data.transformations.Transformations;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class QueryDataSource implements BaselineDataSource {


    private InfluxAccess influx;

    private String queryTemplate;

    public QueryDataSource(InfluxAccess influx, QueryBaselineDefinition settings) {
        this.influx = influx;
        this.queryTemplate = settings.getQuery();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> rawPoints = influx.queryTemplate(queryTemplate, start, end);

        return Transformations.meanByInterval(rawPoints, intervalMillis);
    }
}
