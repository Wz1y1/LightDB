package ed.inf.adbs.lightdb;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {
    private final Operator childOperator;
    private final Expression whereCondition;
    private final Schema schema;

    // Modified constructor to accept columnToIndexMap
    public SelectOperator(Operator childOperator, Expression whereCondition, Schema schema) {
        this.childOperator = childOperator;
        this.whereCondition = whereCondition;
        this.schema = schema;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            SelectEvaluator evaluator = new SelectEvaluator(tuple, schema);
            whereCondition.accept(evaluator);
            if (evaluateCondition(tuple, whereCondition)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
    }

    private boolean evaluateCondition(Tuple tuple, Expression expression) {
        SelectEvaluator evaluator = new SelectEvaluator(tuple, schema);
        expression.accept(evaluator);
        return evaluator.getResult();
    }
}
