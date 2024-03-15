//package ed.inf.adbs.lightdb;
//
//import net.sf.jsqlparser.expression.Expression;
//import net.sf.jsqlparser.parser.CCJSqlParserUtil;
//import java.util.Arrays;
//import java.util.List;
//
//public class ExpressionTest {
//
//    public static void main(String[] args) {
//        try {
//
//            List<String> columnNames = Arrays.asList("A", "B", "C");
//            Schema schema = new Schema(columnNames); // Assuming Schema constructor accepts a list of column names
//            List<Integer> tupleValues = Arrays.asList(1, 42, 4);
//            Tuple tuple = new Tuple(tupleValues); // Assuming Tuple constructor accepts a list of values
//
//
//            // The expression to evaluate
//            String exprStr = "B = 42 AND C > A";
//            Expression expression = CCJSqlParserUtil.parseExpression(exprStr);
//
//            // Evaluate the expression using the TupleExpressionEvaluator
//            SchemaEvaluatorVisitor evaluator = new SchemaEvaluatorVisitor(tuple, schema);
//            expression.accept(evaluator);
//
//            // Print the result
//            System.out.println("Expression result: " + evaluator.getResult());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}