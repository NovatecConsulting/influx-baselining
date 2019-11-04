package de.novatec.baselining.config.baselines;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CounterBaselineDefinition extends AbstractTaggedAggregatingBaselineDefinition{

    @Valid
    private MeasurementFieldName input;

    @NotNull
    private Duration lookBack = Duration.ofMinutes(15);
}