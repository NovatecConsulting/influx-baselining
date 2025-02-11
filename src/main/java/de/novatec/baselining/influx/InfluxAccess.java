package de.novatec.baselining.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteOptions;
import com.influxdb.query.InfluxQLQueryResult;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import lombok.extern.slf4j.Slf4j;
import com.influxdb.client.write.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class InfluxAccess {

    private final InfluxQuery query;

    private final InfluxWrite write;

    @Autowired
    public InfluxAccess(InfluxDBClient influx) {
        // we still use InfluxQL instead of Flux for queries
        this.query = new InfluxQuery(influx.getInfluxQLQueryApi());
        this.write = new InfluxWrite(influx.makeWriteApi(WriteOptions.DEFAULTS));
    }

    /**
     * Query data via configured query template.
     *
     * @param database the database (bucket) to query data
     * @param queryTemplate the configured InfluxQL query template
     * @return the InfluxQL query result mapped to datapoints for each unique tag combination
     */
    public Map<TagValues, List<DataPoint>> queryTemplate(String database, String queryTemplate, long startMillis, long endMillis) {
        return query.queryTemplate(database, queryTemplate, startMillis, endMillis);
    }

    /**
     * Query data via complete query.
     * A warn message may be logged, if no data could be fetched.
     *
     * @param database the database (bucket) to query data
     * @param selectFrom the InfluxQL query
     * @return the InfluxQL query result
     */
    public InfluxQLQueryResult query(String database, String selectFrom, long startMillis, long endMillis) {
        return query.query(database, selectFrom, startMillis, endMillis);
    }

    /**
     * Query data of a single filed via complete query.
     *
     * @param database the database (bucket) to query data
     * @param selectFrom the InfluxQL query
     * @return the InfluxQL query result mapped to datapoints for each unique tag combination
     */
    public Map<TagValues, List<DataPoint>> querySingleField(String database, String selectFrom, long startMillis, long endMillis) {
        return query.querySingleField(database, selectFrom, startMillis, endMillis);
    }

    /**
     * Query data of a single field aggregated within interval via complete query.
     *
     * @param database the database (bucket) to query data
     * @param selectFrom the InfluxQL query
     * @param intervalMillis the aggregation interval
     * @return the InfluxQL query result mapped to datapoints for each unique tag combination
     */
    public Map<TagValues, List<DataPoint>> queryAggregate(String database, String selectFrom, long startMillis, long endMillis, long intervalMillis) {
        return query.queryAggregate(database, selectFrom, startMillis, endMillis, intervalMillis);
    }

    /**
     * Writes the data points into InfluxDB with their tags.
     *
     * @param database the database (bucket) to write data into
     * @param measurement the measurement to write data into
     * @param points the collection of tags and their data points
     */
    public void writePoints(String database, String measurement, Map<TagValues, ? extends Collection<DataPoint>> points) {
        write.writePoints(database, measurement, points);
    }

    /**
     * Writes the data points into InfluxDB with their tags.
     *
     * @param database the database (bucket) to write data into
     * @param tags the collection of tag keys and values
     * @param points the collection of data points
     */
    public void writePoints(String database, Map<String, String> tags, List<Point> points) {
        write.writePoints(database, tags, points);
    }
}
