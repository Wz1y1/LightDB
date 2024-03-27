package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import java.util.*;

/**
 * Processes WHERE clause expressions to segregate table-specific conditions and join conditions.
 * It categorizes expressions based on whether they apply to a single table or multiple tables.
 */
public class WhereClauseProcessor extends ExpressionDeParser {
    private final Map<String, List<Expression>> tableConditions = new HashMap<>();
    private final List<Expression> joinExpressions = new ArrayList<>(); // Adjusted to be a list
    private final Set<String> tablesInvolved = new HashSet<>();

    public WhereClauseProcessor() {
        super();
    }

    /**
     * Retrieves all conditions associated with a specific table.
     *
     * @param tableName The name of the table.
     * @return A list of expressions (conditions) associated with the specified table.
     */
    public List<Expression> getConditionsForTable(String tableName) {
        return tableConditions.getOrDefault(tableName, new ArrayList<>());
    }

    /**
     * Gets the list of join expressions identified in the WHERE clause.
     *
     * @return A list of join conditions.
     */
    public List<Expression> getJoinExpressions() {
        return joinExpressions;
    }

    /**
     * Visits a column reference in an expression to identify the table it belongs to.
     *
     * @param column The column being visited.
     */
    @Override
    public void visit(Column column) {
        super.visit(column);
        tablesInvolved.add(column.getTable().getName());
    }

    /**
     * Appends a condition to the appropriate list (table-specific or join conditions)
     * based on the number of tables involved in the condition.
     *
     * @param condition The condition (expression) to append.
     */
    private void appendCondition(Expression condition) {
        if (tablesInvolved.size() == 1) {
            String tableName = tablesInvolved.iterator().next();
            tableConditions.putIfAbsent(tableName, new ArrayList<>());
            tableConditions.get(tableName).add(condition);
        } else if (tablesInvolved.size() > 1) {
            joinExpressions.add(condition);
        }
        tablesInvolved.clear();
    }

    // The following override methods visit various types of expressions, categorizing them
    // into selection conditions (single-table) or join conditions (multi-table) as appropriate.
    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        appendCondition(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        appendCondition(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        appendCondition(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        appendCondition(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        appendCondition(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        appendCondition(notEqualsTo);
    }


    @Override
    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
        appendCondition(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        super.visit(orExpression);
        appendCondition(orExpression);
    }

}
