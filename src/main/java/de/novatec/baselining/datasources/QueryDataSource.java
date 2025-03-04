package de.novatec.baselining.datasources;

import de.novatec.baselining.config.BaselineServiceSettings;
import de.novatec.baselining.influx.InfluxAccess;
import de.novatec.baselining.config.baselines.QueryBaselineDefinition;
import de.novatec.baselining.data.AggregatePoint;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import de.novatec.baselining.data.transformations.Transformations;
import de.novatec.baselining.influx.InfluxUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class QueryDataSource implements BaselineDataSource {

    private final InfluxAccess influx;

    private final String database;

    private final String queryTemplate;

    public QueryDataSource(InfluxAccess influx, BaselineServiceSettings config, QueryBaselineDefinition settings) {
        this.influx = influx;
        this.database = getDatabase(config, settings);
        this.queryTemplate = settings.getQuery();
    }

    @Override
    public Map<TagValues, List<AggregatePoint>> fetch(long intervalMillis, long startInterval, long endInterval) {
        long start = startInterval * intervalMillis;
        long end = endInterval * intervalMillis;

        Map<TagValues, List<DataPoint>> rawPoints = influx.queryTemplate(database, queryTemplate, start, end);

        return Transformations.meanByInterval(rawPoints, intervalMillis);
    }

    /**
     * Get the database for the {@link com.influxdb.client.domain.InfluxQLQuery}.
     * If a database was specified explicitly in the settings, use the provided value.
     * If no database was specified AND the option to derive it from the query is enabled, extract the database from
     * the query body.
     *
     * @param config the service configuration
     * @param settings the settings for the query baseline
     * @return the database to use for the query
     */
    private String getDatabase(BaselineServiceSettings config, QueryBaselineDefinition settings) {
        if(settings.getDatabase().isBlank() && config.isDeriveDatabaseFromQuery())
            return InfluxUtils.extractDatabase(settings.getQuery());
        else return settings.getDatabase();
    }
}
