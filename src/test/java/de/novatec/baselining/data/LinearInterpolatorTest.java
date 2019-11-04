package de.novatec.baselining.data;

import de.novatec.baselining.data.transformations.LinearInterpolator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class LinearInterpolatorTest {

    @Nested
    public class Interpolate {

        @Test
        void noPoints() {
            LinearInterpolator ip = new LinearInterpolator(Collections.emptyList());

            assertThat(ip.interpolate(50)).isEqualTo(0);
        }

        @Test
        void leftBorder() {
            LinearInterpolator ip = new LinearInterpolator(Arrays.asList(
                    DataPoint.builder().time(50).value(100).build(),
                    DataPoint.builder().time(100).value(200).build()
            ));

            assertThat(ip.interpolate(10)).isEqualTo(100);
        }

        @Test
        void rightBorder() {
            LinearInterpolator ip = new LinearInterpolator(Arrays.asList(
                    DataPoint.builder().time(50).value(100).build(),
                    DataPoint.builder().time(100).value(200).build()
                    ));

            assertThat(ip.interpolate(110)).isEqualTo(200);
        }

        @Test
        void interpolating() {
            LinearInterpolator ip = new LinearInterpolator(Arrays.asList(
                    DataPoint.builder().time(50).value(100).build(),
                    DataPoint.builder().time(100).value(150).build(),
                    DataPoint.builder().time(150).value(170).build(),
                    DataPoint.builder().time(200).value(70).build(),
                    DataPoint.builder().time(210).value(200).build(),
                    DataPoint.builder().time(210).value(100).build()
            ));

            assertThat(ip.interpolate(50)).isEqualTo(100);
            assertThat(ip.interpolate(75)).isEqualTo(125);
            assertThat(ip.interpolate(125)).isEqualTo(160);
            assertThat(ip.interpolate(160)).isEqualTo(150);
        }
    }
}
