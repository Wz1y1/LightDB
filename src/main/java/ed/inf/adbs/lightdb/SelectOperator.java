package ed.inf.adbs.lightdb;
import net.sf.jsqlparser.expression.Expression;

/**
 * The {@code SelectOperator} class extends {@code Operator} to implement selection (filtering) functionality
 * over a stream of tuples according to a specified condition. It acts as a wrapper around another operator,
 * filtering tuples based on the evaluation of a given SQL {@link Expression}.
 */
public class SelectOperator extends Operator {
    private final Operator childOperator; // The child operator to fetch tuples from.
    private final Expression whereCondition; // The SQL condition to evaluate for each tuple.
    private final Schema schema; // The schema used for evaluating column references in the condition.


    /**
     * Constructs a new {@code SelectOperator} with the specified child operator, selection condition,
     * and schema.
     *
     * @param childOperator The child operator providing tuples to filter.
     * @param whereCondition The SQL expression representing the filtering condition.
     * @param schema The schema of the tuples, used for evaluating column references in the condition.
     */
    public SelectOperator(Operator childOperator, Expression whereCondition, Schema schema) {
        this.childOperator = childOperator;
        this.whereCondition = whereCondition;
        this.schema = schema;
    }

    /**
     * Retrieves and returns the next tuple from the child operator that satisfies the selection condition.
     * This method iterates over tuples provided by the child operator, applying the filtering condition to each.
     * The first tuple that satisfies the condition is returned, or {@code null} if no such tuple exists.
     *
     * @return The next {@link Tuple} satisfying the selection condition, or {@code null} if there are no more such tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            if (evaluateCondition(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    /**
     * Resets the state of this operator and its child operator, allowing tuples to be fetched from the beginning.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }

    /**
     * Evaluates the selection condition for a given tuple.
     *
     * @param tuple The tuple to evaluate the selection condition against.
     * @return {@code true} if the tuple satisfies the selection condition, {@code false} otherwise.
     */
    private boolean evaluateCondition(Tuple tuple) {
        // Instantiate a SelectEvaluator with the current tuple and schema.
        SelectEvaluator evaluator = new SelectEvaluator(tuple, schema);
        // Use the visitor pattern to evaluate the whereCondition expression.
        whereCondition.accept(evaluator);
        // Return the result of the condition evaluation.
        return evaluator.getResult();
    }
}
