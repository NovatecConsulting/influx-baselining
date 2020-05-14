package de.novatec.baselining;

import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Slf4j
public class InfluxAccess {

    private static final QueryResult EMPTY_QUERY_RESULT = new QueryResult();

    @Autowired
    private InfluxDB influx;

    private String buildTimeFilter(long startMillis, long endMillis) {
        return new StringBuilder()
                .append(" ( time >= ").append(startMillis).append("000000")
                .append(" AND time < ").append(endMillis).append("000000) ")
                .toString();
    }

    public Map<TagValues, List<DataPoint>> queryTemplate(String queryTemplate, long startMillis, long endMillis) {
        StringLookup lookup = (variable) -> {
            if (variable.equalsIgnoreCase("timeFilter")) {
                return buildTimeFilter(startMillis, endMillis);
            }
            throw new IllegalArgumentException("Unknown query variable: "+variable);
        };
        StringSubstitutor subst = new StringSubstitutor(lookup);
        String queryString = subst.replace(queryTemplate);
        return extractSeriesResults(influx.query(new Query(queryString), TimeUnit.MILLISECONDS));
    }

    public QueryResult query(String selectFrom, long startMillis, long endMillis) {
        return query(selectFrom, null, "*", startMillis, endMillis);
    }

    public QueryResult query(String selectFrom, String filter, String groupBy, long startMillis, long endMillis) {
        StringBuilder query = new StringBuilder(selectFrom);
        query.append(" WHERE ").append(buildTimeFilter(startMillis, endMillis));
        if (!StringUtils.isEmpty(filter)) {
            query.append(" AND ").append(filter);
        }
        query.append(" GROUP BY ").append(groupBy);

        try {
            return influx.query(new Query(query.toString()), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Exception while executing InfluxDB query.", e);
            return EMPTY_QUERY_RESULT;
        }
    }

    public Map<TagValues, List<DataPoint>> querySingleField(String selectFrom, long startMillis, long endMillis) {
        QueryResult queryResult = query(selectFrom, startMillis, endMillis);
        return extractSeriesResults(queryResult);
    }

    public Map<TagValues, List<DataPoint>> queryAggregate(String selectFrom, long startMillis, long endMillis, long intervalMillis) {
        String groupBy = "*, time(" + intervalMillis + "ms) fill(none)";
        QueryResult queryResult = query(selectFrom, null, groupBy, startMillis, endMillis);
        return extractSeriesResults(queryResult);
    }

    private Map<TagValues, List<DataPoint>> extractSeriesResults(QueryResult queryResult) {
        if (queryResult.getError() != null) {
            throw new RuntimeException("Influx Returned Error: " + queryResult.getError());
        }
        if (queryResult.getResults() != null) {
            return queryResult.getResults().stream()
                    .filter(Objects::nonNull)
                    .map(result -> result.getSeries())
                    .filter(Objects::nonNull)
                    .flatMap(List::stream)
                    .filter(series -> series.getValues() != null && !series.getValues().isEmpty())
                    .collect(Collectors.toMap(
                            series -> TagValues.from(series.getTags()),
                            series -> seriesToPoints(series)
                    ));
        }

        return Collections.emptyMap();
    }

    private List<DataPoint> seriesToPoints(QueryResult.Series series) {
        if (series.getColumns().size() != 2) {
            throw new IllegalArgumentException("Query returned more than one non-time field: " + series.getColumns());
        }
        int timeIndex = series.getColumns().indexOf("time");
        int fieldIndex = timeIndex == 0 ? 1 : 0;
        return series.getValues().stream()
                .filter(vals -> vals.get(fieldIndex) != null)
                .map(values -> DataPoint.builder()
                        .time(((Number) values.get(timeIndex)).longValue())
                        .value(((Number) values.get(fieldIndex)).doubleValue())
                        .build())
                .collect(Collectors.toList());
    }

    public void writePoints(String database, String retention, String measurement, Map<TagValues, ? extends Collection<DataPoint>> points) {
        points.forEach((tags, pts) -> {
            List<Point> converted = pts.stream()
                    .map(pt -> Point
                            .measurement(measurement)
                            .time(pt.getTime(), TimeUnit.MILLISECONDS)
                            .addField("value", pt.getValue())
                            .build())
                    .collect(Collectors.toList());
            if (!converted.isEmpty()) {
                writePoints(database, retention, tags.getTags(), converted);
            }
        });
    }

    public void writePoints(String database, String retention, Map<String, String> tags, Collection<Point> points) {
        BatchPoints.Builder builder = BatchPoints.database(database)
                .retentionPolicy(retention)
                .precision(TimeUnit.MILLISECONDS);

        tags.entrySet()
                .stream()
                .filter(entry -> !StringUtils.isEmpty(entry.getValue()))
                .forEach(entry -> builder.tag(entry.getKey(), entry.getValue()));
        builder.points(points);

        try {
            influx.write(builder.build());
        } catch (Exception e) {
            log.error("Exception while writing InfluxDB data.", e);
        }
    }

}
