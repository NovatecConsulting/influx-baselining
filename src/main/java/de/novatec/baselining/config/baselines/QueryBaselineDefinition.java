package de.novatec.baselining.config.baselines;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryBaselineDefinition extends AbstractBaselineDefinition {

    @NotNull
    private String database = "";

    @NotBlank
    private String query;

}
