package io.streamthoughts.kafka.connect.filepulse.expression.function.strings;

import io.streamthoughts.kafka.connect.filepulse.data.Type;
import io.streamthoughts.kafka.connect.filepulse.data.TypedValue;
import io.streamthoughts.kafka.connect.filepulse.expression.Expression;
import io.streamthoughts.kafka.connect.filepulse.expression.StandardEvaluationContext;
import org.junit.Assert;
import org.junit.Test;

import static io.streamthoughts.kafka.connect.filepulse.expression.parser.ExpressionParsers.parseExpression;

public class IsEmptyTest {
    private static final StandardEvaluationContext EMPTY_CONTEXT  = new StandardEvaluationContext(new Object());

    @Test
    public void should_return_false_with_not_empty_field() {
        Expression expression =  parseExpression("{{ is_empty('notEmpty') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals(Type.BOOLEAN, result.type());
        Assert.assertEquals(result.value(), false);
    }

    @Test
    public void should_return_true_with_empty_field() {
        Expression expression =  parseExpression("{{ is_empty('') }}");
        TypedValue result = expression.readValue(EMPTY_CONTEXT, TypedValue.class);
        Assert.assertEquals(Type.BOOLEAN, result.type());
        Assert.assertEquals(result.value(), true);
    }
}