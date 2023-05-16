package de.novatec.baselining.config.baselines;

import de.novatec.baselining.config.measurement.MeasurementName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AbstractTaggedAggregatingBaselineDefinition extends AbstractBaselineDefinition {

    private List<@NotBlank String> tags;

    @NotNull
    private Duration samplePrecision = Duration.ofSeconds(15);

    private boolean loopBackSrc = true;

    @AssertTrue
    public boolean isPrecisionMultipleOfSamplePrecision() {
        return getPrecision().toMillis() % samplePrecision.toMillis() == 0;
    }

    public MeasurementName getLoopBackMetric() {
        if (loopBackSrc) {
            return getOutput().toBuilder()
                    .measurement(getOutput().getMeasurement() + "_src")
                    .build();
        } else {
            return null;
        }
    }
}

