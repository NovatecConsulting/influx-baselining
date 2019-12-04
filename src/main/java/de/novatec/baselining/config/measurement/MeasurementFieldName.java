package de.novatec.baselining.config.measurement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class MeasurementFieldName extends MeasurementName {

    @NotBlank
    private String field;

    public MeasurementFieldName(String database, String retention, String measurement, String field) {
        super(database, retention, measurement);
        this.field = field;
    }
}
