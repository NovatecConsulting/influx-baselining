package de.novatec.baselining.influx;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class InfluxUtils {

    public static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    public static final long MILLIS_PER_HOUR = Duration.ofHours(1).toMillis();
    public static final long MILLIS_PER_MINUTE = Duration.ofMinutes(1).toMillis();
    public static final long MILLIS_PER_SECOND = Duration.ofSeconds(1).toMillis();

    /**
     * Basically, extract the string after a FROM statement, which is enclosed by double quotes.
     * For example: SELECT * FROM "inspectit"."raw"."measure" --> inspectit
     */
    private static final String DATABASE_PATTERN = "(?i)\\bFROM\\s+\"([^\"]+)";

    /**
     * @param query the InfluxQL query body
     * @return the database
     * @throws IllegalArgumentException if no database could be extracted
     */
    public static String extractDatabase(String query) {
        Pattern pattern = Pattern.compile(DATABASE_PATTERN);
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            String database = matcher.group(1);
            log.debug("Extracted database from query: {}", database);
            return database;
        }
        else throw new IllegalArgumentException("No match found for database. You can specify the database directly as well");
    }

    public static String prettyPrintDuration(long millis) {
        long timeLeft = millis;
        StringBuilder result = new StringBuilder();
        if (timeLeft / MILLIS_PER_DAY > 0) {
            result.append(timeLeft / MILLIS_PER_DAY).append("d");
            timeLeft = timeLeft % MILLIS_PER_DAY;
        }
        if (timeLeft / MILLIS_PER_HOUR > 0) {
            result.append(timeLeft / MILLIS_PER_HOUR).append("h");
            timeLeft = timeLeft % MILLIS_PER_HOUR;
        }
        if (timeLeft / MILLIS_PER_MINUTE > 0) {
            result.append(timeLeft / MILLIS_PER_MINUTE).append("m");
            timeLeft = timeLeft % MILLIS_PER_MINUTE;
        }
        if (timeLeft / MILLIS_PER_SECOND > 0) {
            result.append(timeLeft / MILLIS_PER_SECOND).append("s");
            timeLeft = timeLeft % MILLIS_PER_SECOND;
        }
        if (timeLeft > 0 || result.length() == 0) {
            result.append(timeLeft).append("ms");
        }
        return result.toString();
    }
}
