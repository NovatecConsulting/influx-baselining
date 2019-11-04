package de.novatec.baselining.config.baselines;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

@Data()
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class GaugeBaselineDefinition extends AbstractTaggedAggregatingBaselineDefinition{

    @Valid
    @NotNull
    private MeasurementFieldName input;
}
