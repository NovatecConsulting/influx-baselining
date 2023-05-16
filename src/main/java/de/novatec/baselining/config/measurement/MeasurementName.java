package de.novatec.baselining.config.measurement;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class MeasurementName {

    @NotBlank
    private String database;

    @NotBlank
    private String retention;

    @NotBlank
    private String measurement;

    public String getFullMeasurementName() {
        return database + "." + retention + "." + measurement;
    }
}
