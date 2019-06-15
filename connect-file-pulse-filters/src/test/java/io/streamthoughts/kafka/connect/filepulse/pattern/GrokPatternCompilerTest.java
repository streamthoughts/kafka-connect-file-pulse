package io.streamthoughts.kafka.connect.filepulse.pattern;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class GrokPatternCompilerTest {

    private GrokPatternCompiler compiler;

    @Before
    public void setUp() {
        compiler = new GrokPatternCompiler(new GrokPatternResolver(), false);
    }

    @Test
    public void shouldCompileMatcherGivenSingleGrokPattern() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE}");
        Assert.assertNotNull(matcher);
        Assert.assertEquals("ISO8601_TIMEZONE", matcher.getGrokPattern(0).syntax());
    }

    @Test
    public void shouldCompileMatcherGivenMultipleGrokPatterns() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE} %{LOGLEVEL} %{GREEDYDATA}");
        Assert.assertNotNull(matcher);
        Assert.assertNotNull(matcher.getGrokPattern("ISO8601_TIMEZONE"));
        Assert.assertNotNull(matcher.getGrokPattern("LOGLEVEL"));
        Assert.assertNotNull(matcher.getGrokPattern("GREEDYDATA"));
    }

    @Test
    public void shouldCompileMatcherGivenMultipleGrokPatternWithSemantic() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE:timezone}");
        Assert.assertNotNull(matcher);
        Assert.assertEquals("ISO8601_TIMEZONE", matcher.getGrokPattern(0).syntax());
        Assert.assertEquals("timezone", matcher.getGrokPattern(0).semantic());
    }

    @Test
    public void shouldCompileMatcherGivenMultipleGrokPatternWithSemanticAndType() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE:timezone:integer}");
        Assert.assertNotNull(matcher);
        Assert.assertEquals("ISO8601_TIMEZONE", matcher.getGrokPattern(0).syntax());
        Assert.assertEquals("timezone", matcher.getGrokPattern(0).semantic());
        Assert.assertEquals(Type.INTEGER, matcher.getGrokPattern(0).type());
    }
}