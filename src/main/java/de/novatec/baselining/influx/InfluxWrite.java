package de.novatec.baselining.influx;

import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WriteConsistency;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.client.write.WriteParameters;
import de.novatec.baselining.data.DataPoint;
import de.novatec.baselining.data.TagValues;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class InfluxWrite {

    private final WriteApi writeApi;

    public InfluxWrite(WriteApi writeApi) {
        this.writeApi = writeApi;
    }

    public void writePoints(String database, String measurement, Map<TagValues, ? extends Collection<DataPoint>> points) {
        points.forEach((tags, pts) -> {
            List<Point> converted = pts.stream()
                    .map(pt -> new Point(measurement)
                            .time(pt.getTime(), WritePrecision.MS)
                            .addField("value", pt.getValue())
                    )
                    .collect(Collectors.toList());
            if (!converted.isEmpty()) {
                writePoints(database, tags.getTags(), converted);
            }
        });
    }

    public void writePoints(String database, Map<String, String> tags, List<Point> points) {
        // writing in chunks
        int chunkSize = 25_000;
        int startIndex = 0;
        int endIndex = Math.min(points.size(), chunkSize);
        boolean done = false;

        while (!done) {
            List<Point> chunk = points.subList(startIndex, endIndex);

            tags.entrySet().stream()
                    .filter(entry -> !ObjectUtils.isEmpty(entry.getValue()))
                    .forEach(entry -> chunk.forEach(p -> p.addTag(entry.getKey(), entry.getValue())));

            // Read org from influx configuration
            WriteParameters parameters = new WriteParameters(database, null, WritePrecision.MS, WriteConsistency.ONE);
            writePoints(chunk, parameters);

            if (endIndex == points.size()) {
                done = true;
            } else {
                startIndex = endIndex;
                endIndex = Math.min(points.size(), (endIndex + chunkSize));
            }
        }
    }

    private void writePoints(List<Point> points, WriteParameters writeParameters) {
        log.info("Writing {} points into the InfluxDB", points.size());
        try {
            writeApi.writePoints(points, writeParameters);
        } catch (Exception first) {
            try {
                log.error("Exception while writing InfluxDB data but it is tried once more in 2 seconds.");
                Thread.sleep(2000);
                writeApi.writePoints(points);
            } catch (Exception second) {
                log.error("Exception while writing InfluxDB data.", second);
            }
        }
    }
}
