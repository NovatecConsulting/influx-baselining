package de.novatec.baselining.data;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.Collection;
import java.util.Map;

@Builder
@Value
public class TagValues {

    @Singular
    private Map<String, String> tags;

    public static TagValues from(Map<String, String> tags) {
        if (tags == null) {
            return TagValues.builder().build();
        } else {
            return TagValues.builder().tags(tags).build();
        }
    }

    public TagValues keepOnly(Collection<String> tagKeys) {
        TagValues.TagValuesBuilder result = TagValues.builder();
        for (String tag : tagKeys) {
            if (tags.containsKey(tag)) {
                result.tag(tag, tags.get(tag));
            }
        }
        return result.build();
    }
}
