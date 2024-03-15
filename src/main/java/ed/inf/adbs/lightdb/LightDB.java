package ed.inf.adbs.lightdb;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		executeSQL(databaseDir, inputFile, outputFile);
	}


//	public static void executeSQL(String databaseDir, String inputFile, String outputFile) {
//		try {
//			DatabaseCatalog catalog = DatabaseCatalog.getInstance();
//			// Initialize the catalog using the new method
//			catalog.initializeCatalog(databaseDir, databaseDir + "/schema.txt");
//
//			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
//			if (statement instanceof Select) {
//				Select select = (Select) statement;
//				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
//				Table table = (Table) plainSelect.getFromItem();
//				String tableName = table.getName();
//
//				Expression whereCondition = plainSelect.getWhere();
//
//				List<String> columnNames = catalog.getSchema(tableName); // This should return column names without the table prefix
//				Schema schema = new Schema(tableName, columnNames);
//
//
//				// Create the ScanOperator to scan the table
//				Operator scanOperator = new ScanOperator(tableName);
//				Operator currentOperator = scanOperator;
//				System.out.println("Where condition: " + whereCondition);
//				// Temporary test to print out all tuples scanned
//
//				if (whereCondition != null) {
//					currentOperator = new SelectOperator(scanOperator, whereCondition, schema);
//				}
//				// Handle the SELECT columns (projection)
//				List<SelectItem> selectItems = plainSelect.getSelectItems();
//				if (!isSelectAll(selectItems)) { // Only create a ProjectOperator if specific columns are requested
//					List<String> projectionColumns = parseSelectItems(selectItems);
//					currentOperator = new ProjectOperator(currentOperator, schema, projectionColumns);
//				}
//
//				currentOperator.dump(System.out); // Output filtered tuples to the console
//			}
//		} catch (Exception e) {
//			System.err.println("Exception occurred during selection");
//			e.printStackTrace();
//		}
//	}

	public static void executeSQL(String databaseDir, String inputFile, String outputFile) {
		try {
			DatabaseCatalog catalog = DatabaseCatalog.getInstance();
			catalog.initializeCatalog(databaseDir, databaseDir + "/schema.txt");

			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (statement instanceof Select) {
				Select select = (Select) statement;
				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

				// Handling the FROM item
				Table leftTable = (Table) plainSelect.getFromItem();
				String leftTableName = leftTable.getName();
				List<String> leftColumnNames = catalog.getSchema(leftTableName);
				Schema leftSchema = new Schema(leftTableName, leftColumnNames);
				Operator finalOperator = new ScanOperator(leftTableName);

				Schema combinedSchema = leftSchema; // Initialize combinedSchema with leftSchema

				// Apply WHERE condition if present
				Expression whereCondition = plainSelect.getWhere();
				if (whereCondition != null) {
					finalOperator = new SelectOperator(finalOperator, whereCondition, leftSchema);
				}

				// Handling JOINs
				if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
					for (Join join : plainSelect.getJoins()) {
						Table rightTable = (Table) join.getRightItem();
						String rightTableName = rightTable.getName();
						List<String> rightColumnNames = catalog.getSchema(rightTableName);
						Schema rightSchema = new Schema(rightTableName, rightColumnNames);
						Operator rightScanOperator = new ScanOperator(rightTableName);

						combinedSchema = leftSchema.combineWith(rightSchema); // Update combinedSchema

						// Extracting join condition
						Expression joinCondition = join.getOnExpression();
						finalOperator = new JoinOperator(finalOperator, rightScanOperator, joinCondition);
					}
				}

				// Handle projection
				List<SelectItem> selectItems = plainSelect.getSelectItems();
				if (!isSelectAll(selectItems)) {
					List<String> projectionColumns = parseSelectItems(selectItems);
					finalOperator = new ProjectOperator(finalOperator, combinedSchema, projectionColumns); // Use combinedSchema here
				}

				// Assuming dump method outputs the tuples
				finalOperator.dump(System.out);
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during query execution");
			e.printStackTrace();
		}
	}

//	private static Schema combineSchemas(Schema leftSchema, Schema rightSchema) {
//		// Combine the column names from both schemas
//		List<String> combinedColumnNames = new ArrayList<>(leftSchema.getColumnNames());
//		combinedColumnNames.addAll(rightSchema.getColumnNames());
//		return new Schema("Combined", combinedColumnNames);
//	}



	private static boolean isSelectAll(List<SelectItem> selectItems) {
		// Check if the query is "SELECT *"
		return selectItems.size() == 1 && selectItems.get(0) instanceof AllColumns;
	}

	private static List<String> parseSelectItems(List<SelectItem> selectItems) {
		// Extract column names from SelectItems for projection
		List<String> columns = new ArrayList<>();
		for (SelectItem item : selectItems) {
			if (item instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) item).getExpression();
				if (expression instanceof Column) {
					columns.add(((Column) expression).getFullyQualifiedName());
				}
			}
		}
		return columns;
	}
}
