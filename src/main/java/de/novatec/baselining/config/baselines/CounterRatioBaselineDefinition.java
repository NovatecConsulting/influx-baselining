package de.novatec.baselining.config.baselines;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CounterRatioBaselineDefinition extends AbstractTaggedAggregatingBaselineDefinition {

    @NotNull
    @Valid
    private MeasurementFieldName input;

    @NotNull
    @Valid
    private MeasurementFieldName divideBy;

    private Duration lookBack = Duration.ofMinutes(15);
}
