package de.novatec.baselining.config.baselines;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Duration;

@Data
@NoArgsConstructor
public class OutlierRemovalSettings {

    /**
     * Remove every point which is above the given percentile
     */
    private double percentile = 1.0;

    private Duration window = Duration.ofMinutes(5);

    private long minPointCount = 3;
}
