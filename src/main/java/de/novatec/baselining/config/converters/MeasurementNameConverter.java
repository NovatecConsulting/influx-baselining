package de.novatec.baselining.config.converters;

import de.novatec.baselining.config.measurement.MeasurementName;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@ConfigurationPropertiesBinding
public class MeasurementNameConverter implements Converter<String, MeasurementName> {

    @Override
    public MeasurementName convert(String source) {
        String[] segements = source.split("\\.");
        if(segements.length != 3) {
            throw new IllegalArgumentException("'"+source+"' must have the form '<database>.<retention>.<measurement>'");
        }
        return new MeasurementName(segements[0].trim(), segements[1].trim(), segements[2].trim());
    }
}
