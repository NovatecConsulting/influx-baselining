package de.novatec.baselining.config.baselines;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class RateBaselineDefinition extends AbstractBaselineDefinition {

    private String input;

    private String field = "value";

    private List<String> tags;

    private OutlierRemovalSettings outliers = new OutlierRemovalSettings();
}
