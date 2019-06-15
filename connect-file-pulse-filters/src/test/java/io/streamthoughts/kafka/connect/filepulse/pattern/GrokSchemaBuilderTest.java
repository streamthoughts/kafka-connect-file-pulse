package io.streamthoughts.kafka.connect.filepulse.pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.streamthoughts.kafka.connect.filepulse.pattern.GrokSchemaBuilder.buildSchemaForGrok;

public class GrokSchemaBuilderTest {

    private static final String GROK_PATTERN = "(?<timestamp>%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{TIME}) %{LOGLEVEL:level} %{GREEDYDATA:message}";

    @Test
    public void test() {
        final GrokPatternCompiler compiler = new GrokPatternCompiler(new GrokPatternResolver(), true);
        final GrokMatcher matcher = compiler.compile(GROK_PATTERN);
        Schema schema = buildSchemaForGrok(Collections.singletonList(matcher));
        List<Field> fields = schema.schema().fields();
        Assert.assertEquals(3, fields.size());
        Assert.assertEquals("timestamp", fields.get(0).name());
        Assert.assertEquals("level", fields.get(1).name());
        Assert.assertEquals("message", fields.get(2).name());
    }
}