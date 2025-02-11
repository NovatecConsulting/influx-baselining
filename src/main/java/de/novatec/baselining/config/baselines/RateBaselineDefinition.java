package de.novatec.baselining.config.baselines;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import jakarta.validation.Valid;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.List;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class RateBaselineDefinition extends AbstractBaselineDefinition {

    @Valid
    @NotNull
    private MeasurementFieldName input;

    private List<String> tags;

    private OutlierRemovalSettings outliers = new OutlierRemovalSettings();
}
