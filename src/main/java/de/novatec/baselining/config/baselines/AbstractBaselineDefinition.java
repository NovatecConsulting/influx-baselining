package de.novatec.baselining.config.baselines;

import de.novatec.baselining.config.measurement.MeasurementName;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.time.DurationMin;

import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
public class AbstractBaselineDefinition {
    @NotNull
    @Valid
    private MeasurementName output;

    @DurationMin(millis = 1)
    private Duration precision = Duration.ofMinutes(15);

    @NotNull
    @DurationMin(millis = 1)
    private Duration seasonality;

    private List<@DurationMin(millis = 1) Duration> windows;

    @AssertTrue
    public boolean isSeasonalityMultipleOfPrecision() {
        return seasonality.toMillis() % precision.toMillis() == 0;
    }

    @AssertTrue
    public boolean isWindowsMultiplesOfSeasonality() {
        if (windows != null) {
            for (Duration window : windows) {
                if (window.toMillis() % seasonality.toMillis() != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    public List<Duration> getWindowsWithDefault() {
        if (windows == null) {
            return Collections.singletonList(seasonality.multipliedBy(10));
        } else {
            return windows;
        }
    }
}
