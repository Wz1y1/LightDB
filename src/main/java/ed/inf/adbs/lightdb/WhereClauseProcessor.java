//package ed.inf.adbs.lightdb;
//
//import net.sf.jsqlparser.expression.*;
//import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
//import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
//import net.sf.jsqlparser.expression.operators.relational.*;
//import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
//import java.util.*;
//
//public class WhereClauseProcessor extends ExpressionDeParser {
//    private final Set<String> tablesInvolved = new HashSet<>();
//    private Expression selectionExpression = null;
//    private Expression joinExpression = null;
//
//    public WhereClauseProcessor() {
//        super();
//    }
//
//    // Getters
//    public Expression getSelectionExpression() {
//        return selectionExpression;
//    }
//
//    public Expression getJoinExpression() {
//        return joinExpression;
//    }
//
//    @Override
//    public void visit(Column column) {
//        super.visit(column);
//        tablesInvolved.add(column.getTable().getName());
//    }
//
//    @Override
//    public void visit(EqualsTo equalsTo) {
//        super.visit(equalsTo); // This ensures child expressions are visited
//
//        // After visiting, reset tablesInvolved for the next expression
//        if (tablesInvolved.size() == 1) {
//            // This is a selection condition
//            selectionExpression = appendExpression(selectionExpression, equalsTo);
//        } else if (tablesInvolved.size() > 1) {
//            // This is a join condition
//            joinExpression = appendExpression(joinExpression, equalsTo);
//        }
//        tablesInvolved.clear();
//    }
//
//    @Override
//    public void visit(GreaterThan greaterThan) {
//        super.visit(greaterThan); // This ensures child expressions are visited
//
//        // Determine if this is a selection or join condition
//        if (tablesInvolved.size() == 1) {
//            selectionExpression = appendExpression(selectionExpression, greaterThan);
//        } else if (tablesInvolved.size() > 1) {
//            joinExpression = appendExpression(joinExpression, greaterThan);
//        }
//        tablesInvolved.clear();
//    }
//
//    @Override
//    public void visit(GreaterThanEquals greaterThanEquals) {
//        super.visit(greaterThanEquals); // This ensures child expressions are visited
//
//        // Determine if this is a selection or join condition
//        if (tablesInvolved.size() == 1) {
//            selectionExpression = appendExpression(selectionExpression, greaterThanEquals);
//        } else if (tablesInvolved.size() > 1) {
//            joinExpression = appendExpression(joinExpression, greaterThanEquals);
//        }
//        tablesInvolved.clear();
//    }
//
//    @Override
//    public void visit(MinorThan lessThan) {
//        super.visit(lessThan); // This ensures child expressions are visited
//
//        // Determine if this is a selection or join condition
//        if (tablesInvolved.size() == 1) {
//            selectionExpression = appendExpression(selectionExpression, lessThan);
//        } else if (tablesInvolved.size() > 1) {
//            joinExpression = appendExpression(joinExpression, lessThan);
//        }
//        tablesInvolved.clear();
//    }
//
//    @Override
//    public void visit(MinorThanEquals lessThanEquals) {
//        super.visit(lessThanEquals); // This ensures child expressions are visited
//
//        // Determine if this is a selection or join condition
//        if (tablesInvolved.size() == 1) {
//            selectionExpression = appendExpression(selectionExpression, lessThanEquals);
//        } else if (tablesInvolved.size() > 1) {
//            joinExpression = appendExpression(joinExpression, lessThanEquals);
//        }
//        tablesInvolved.clear();
//    }
//
//    @Override
//    public void visit(AndExpression andExpression) {
//        // Process the left and right sides of the AND expression separately to maintain logical structure
//        Expression leftSide = processSubExpression(andExpression.getLeftExpression());
//        Expression rightSide = processSubExpression(andExpression.getRightExpression());
//
//        // Combine processed sides using a new AndExpression, if both sides are non-null
//        if (leftSide != null && rightSide != null) {
//            Expression combined = new AndExpression(leftSide, rightSide);
//            appendToAppropriateExpression(combined);
//        } else if (leftSide != null) {
//            // Only the left side is non-null
//            appendToAppropriateExpression(leftSide);
//        } else if (rightSide != null) {
//            // Only the right side is non-null
//            appendToAppropriateExpression(rightSide);
//        }
//    }
//
//    @Override
//    public void visit(OrExpression orExpression) {
//        // Similar logic to AndExpression, but handling OR conditions
//        Expression leftSide = processSubExpression(orExpression.getLeftExpression());
//        Expression rightSide = processSubExpression(orExpression.getRightExpression());
//
//        if (leftSide != null && rightSide != null) {
//            Expression combined = new OrExpression(leftSide, rightSide);
//            appendToAppropriateExpression(combined);
//        } else if (leftSide != null) {
//            appendToAppropriateExpression(leftSide);
//        } else if (rightSide != null) {
//            appendToAppropriateExpression(rightSide);
//        }
//    }
//
//    private Expression processSubExpression(Expression expression) {
//        // Reset tables involved for this sub-expression
//        this.tablesInvolved.clear();
//
//        // Visit the expression to process it
//        expression.accept(this);
//
//        // Determine if this sub-expression is a selection or join based on tables involved
//        if (this.tablesInvolved.size() <= 1) {
//            // This indicates a selection condition
//            Expression result = this.selectionExpression;
//            this.selectionExpression = null; // Reset for subsequent processing
//            return result;
//        } else {
//            // This indicates a join condition
//            Expression result = this.joinExpression;
//            this.joinExpression = null; // Reset for subsequent processing
//            return result;
//        }
//    }
//
//    private void appendToAppropriateExpression(Expression expression) {
//        // Logic to append the expression to either selectionExpression or joinExpression based on tables involved
//        if (this.tablesInvolved.size() <= 1) {
//            this.selectionExpression = appendExpression(this.selectionExpression, expression);
//        } else {
//            this.joinExpression = appendExpression(this.joinExpression, expression);
//        }
//        // Clear tables involved as we're done processing this sub-expression
//        this.tablesInvolved.clear();
//    }
//
//
//
//
//    private Expression appendExpression(Expression existingExpression, Expression newExpression) {
//        if (existingExpression == null) {
//            return newExpression;
//        } else {
//            return new AndExpression(existingExpression, newExpression);
//        }
//    }
//
//}

package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import java.util.*;

public class WhereClauseProcessor extends ExpressionDeParser {
    private Map<String, List<Expression>> tableConditions = new HashMap<>();
    private Expression joinExpression = null;

    private Set<String> tablesInvolved = new HashSet<>();


    public WhereClauseProcessor() {
        super();
    }

    // Method to get all conditions for a specific table
    public List<Expression> getConditionsForTable(String tableName) {
        return tableConditions.getOrDefault(tableName, new ArrayList<>());
    }

    public Expression getJoinExpression() {
        return joinExpression;
    }

    @Override
    public void visit(Column column) {
        super.visit(column);
        // Note: this.tablesInvolved logic should be refined if necessary to accurately track table-column relationships
        tablesInvolved.add(column.getTable().getName());
    }

    // Append conditions to the appropriate list or expression based on the number of tables involved
    private void appendCondition(Expression condition) {
        if (tablesInvolved.size() == 1) {
            // This is a selection condition for a single table
            String tableName = tablesInvolved.iterator().next();
            tableConditions.putIfAbsent(tableName, new ArrayList<>());
            tableConditions.get(tableName).add(condition);
        } else if (tablesInvolved.size() > 1) {
            // This is a join condition involving multiple tables
            joinExpression = appendExpression(joinExpression, condition);
        }
        tablesInvolved.clear();
    }

    // Override methods for different types of expressions to categorize them as selection or join conditions
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
    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
        // Note: Handling of AND expressions may need to account for complex conditions across multiple tables
        appendCondition(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        super.visit(orExpression);
        // Note: Handling of OR expressions may also need to account for complex conditions
        appendCondition(orExpression);
    }

    // Combine expressions using AND, creating a new combined expression
    private Expression appendExpression(Expression existingExpression, Expression newExpression) {
        if (existingExpression == null) {
            return newExpression;
        } else {
            return new AndExpression(existingExpression, newExpression);
        }
    }

    // Additional methods for other expressions (e.g., InExpression, Between, etc.) as needed
    // ...
}

