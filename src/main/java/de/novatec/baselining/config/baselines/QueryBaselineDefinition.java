package de.novatec.baselining.config.baselines;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryBaselineDefinition extends AbstractBaselineDefinition {

    @NotBlank
    private String database;

    @NotBlank
    private String query;

}
