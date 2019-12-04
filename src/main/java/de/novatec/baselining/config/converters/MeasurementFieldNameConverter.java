package de.novatec.baselining.config.converters;

import de.novatec.baselining.config.measurement.MeasurementFieldName;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@ConfigurationPropertiesBinding
public class MeasurementFieldNameConverter implements Converter<String, MeasurementFieldName> {

    @Override
    public MeasurementFieldName convert(String source) {
        String[] segments = source.split("\\.");
        if (segments.length != 4) {
            throw new IllegalArgumentException("'" + source + "' must have the form '<database>.<retention>.<measurement>.<field>'");
        }
        return new MeasurementFieldName(segments[0].trim(), segments[1].trim(), segments[2].trim(), segments[3].trim());
    }
}
