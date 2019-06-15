package io.streamthoughts.kafka.connect.filepulse.pattern;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class GrokPatternResolverTest {

    @Test
    public void shouldLoadAllGrokPatternsFromClasspath() {
        GrokPatternResolver resolver = new GrokPatternResolver();
        resolver.print();
        Assert.assertFalse(resolver.isEmpty());
    }

    @Test
    public void shouldStandardResolveGrokPattern() {
        GrokPatternResolver resolver = new GrokPatternResolver();

        String resolve = resolver.resolve("SYSLOGFACILITY");
        System.out.println(resolve);
    }

}