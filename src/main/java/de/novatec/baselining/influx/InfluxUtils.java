package de.novatec.baselining.influx;

import java.time.Duration;

public class InfluxUtils {

    public static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    public static final long MILLIS_PER_HOUR = Duration.ofHours(1).toMillis();
    public static final long MILLIS_PER_MINUTE = Duration.ofMinutes(1).toMillis();
    public static final long MILLIS_PER_SECOND = Duration.ofSeconds(1).toMillis();

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
