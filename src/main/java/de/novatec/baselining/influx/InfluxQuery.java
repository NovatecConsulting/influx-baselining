package de.novatec.baselining.influx;

import com.influxdb.client.InfluxQLQueryApi;
import com.influxdb.client.domain.InfluxQLQuery;
import com.influxdb.query.InfluxQLQueryResult;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.springframework.util.ObjectUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class InfluxQuery {

    private static final InfluxQLQueryResult EMPTY_QUERY_RESULT = new InfluxQLQueryResult(Collections.emptyList());

    private final InfluxQLQueryApi queryApi;

    public InfluxQuery(InfluxQLQueryApi queryApi) {
        this.queryApi = queryApi;
    }

    public Map<TagValues, List<DataPoint>> queryTemplate(String database, String queryTemplate, long startMillis, long endMillis) {
        StringLookup lookup = (variable) -> {
            if (variable.equalsIgnoreCase("timeFilter")) {
                return buildTimeFilter(startMillis, endMillis);
            }
            throw new IllegalArgumentException("Unknown query variable: " + variable);
        };
        StringSubstitutor subst = new StringSubstitutor(lookup);
        String queryString = subst.replace(queryTemplate);

        InfluxQLQuery influxQLQuery = new InfluxQLQuery(queryString, database);
        InfluxQLQueryResult result = queryApi.query(influxQLQuery);
        return extractSeriesResults(result);
    }

    public InfluxQLQueryResult query(String database, String selectFrom, long startMillis, long endMillis) {
        return query(database, selectFrom, null, "*", startMillis, endMillis);
    }

    public InfluxQLQueryResult query(String database, String selectFrom, String filter, String groupBy, long startMillis, long endMillis) {
        StringBuilder query = new StringBuilder(selectFrom);
        query.append(" WHERE ").append(buildTimeFilter(startMillis, endMillis));
        if (!ObjectUtils.isEmpty(filter)) {
            query.append(" AND ").append(filter);
        }
        query.append(" GROUP BY ").append(groupBy);
        InfluxQLQuery influxQLQuery = new InfluxQLQuery(query.toString(), database);
        try {
            return queryApi.query(influxQLQuery);
        } catch (Exception e) {
            log.error("Exception while executing InfluxDB query.", e);
            return EMPTY_QUERY_RESULT;
        }
    }

    public Map<TagValues, List<DataPoint>> querySingleField(String database, String selectFrom, long startMillis, long endMillis) {
        InfluxQLQueryResult queryResult = query(database, selectFrom, startMillis, endMillis);
        return extractSeriesResults(queryResult);
    }

    public Map<TagValues, List<DataPoint>> queryAggregate(String database, String selectFrom, long startMillis, long endMillis, long intervalMillis) {
        String groupBy = "*, time(" + intervalMillis + "ms) fill(none)";
        InfluxQLQueryResult queryResult = query(database, selectFrom, null, groupBy, startMillis, endMillis);
        return extractSeriesResults(queryResult);
    }

    private Map<TagValues, List<DataPoint>> extractSeriesResults(InfluxQLQueryResult queryResult) {
        return queryResult.getResults()
                .stream()
                .filter(Objects::nonNull)
                .map(InfluxQLQueryResult.Result::getSeries)
                .flatMap(List::stream)
                .filter(series -> !series.getValues().isEmpty())
                .collect(Collectors.toMap(series -> TagValues.from(series.getTags()), this::seriesToPoints));
    }

    private List<DataPoint> seriesToPoints(InfluxQLQueryResult.Series series) {
        if (series.getColumns().size() != 2) {
            throw new IllegalArgumentException("Query returned more than one non-time field: " + series.getColumns());
        }
        int timeIndex = series.getColumns().get("time");
        int fieldIndex = timeIndex == 0 ? 1 : 0;

        return series.getValues().stream()
                .filter(record -> record.getValues()[fieldIndex] != null)
                .map(record -> createDataPoint(record, timeIndex, fieldIndex))
                .collect(Collectors.toList());
    }

    private DataPoint createDataPoint(InfluxQLQueryResult.Series.Record record, int timeIndex, int fieldIndex) {
        Object[] values = record.getValues();
        Object time = values[timeIndex];
        Object value = values[fieldIndex];
        DataPoint.DataPointBuilder builder = DataPoint.builder();
        try {
            // convert nanos to millis
            long timeMillis = Long.parseLong(time.toString()) / 1000 / 1000;
            double resultValue = Double.parseDouble(value.toString());
            builder.time(timeMillis).value(resultValue);
        } catch (NumberFormatException e) {
            // Ignore value
        }
        return builder.build();
    }

    private String buildTimeFilter(long startMillis, long endMillis) {
        return new StringBuilder().append(" ( time >= ")
                .append(startMillis)
                .append("000000")
                .append(" AND time < ")
                .append(endMillis)
                .append("000000) ")
                .toString();
    }
}
