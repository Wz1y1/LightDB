package ed.inf.adbs.lightdb;

import java.io.FileReader;

import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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


	public static void executeSQL(String databaseDir, String inputFile, String outputFile) {
		try {
			DatabaseCatalog catalog = DatabaseCatalog.getInstance();
			catalog.initializeCatalog(databaseDir, databaseDir + "/schema.txt");

			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (statement instanceof Select) {
				Select select = (Select) statement;
				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

				// Initialize AliasHandler and WhereClauseProcessor
				AliasHandler aliasHandler = new AliasHandler(plainSelect);
				WhereClauseProcessor clauseProcessor = new WhereClauseProcessor();
				if (plainSelect.getWhere() != null) {
					plainSelect.getWhere().accept(clauseProcessor);
				}

				Operator currentOperator = null;
				Schema combinedSchema = null;



				// Iterate over tables, now considering aliases
				for (TableInfo tableInfo : extractTables(plainSelect, catalog, aliasHandler)) {
					String tableName = tableInfo.getTableName();
					String aliasName = tableInfo.getAlias();
					String resolvedTableName = aliasName != null ? aliasName : tableName;

					List<String> columnNames = catalog.getSchema(tableName); // Assuming schema is identified by original table name
					Schema schema = new Schema(resolvedTableName, columnNames);

					Operator scanOperator = new ScanOperator(tableName);

					// Use resolvedTableName to fetch conditions, assuming conditions are keyed by alias if present
					List<Expression> tableSpecificConditions = clauseProcessor.getConditionsForTable(resolvedTableName);
					if (tableSpecificConditions != null) {
						for (Expression condition : tableSpecificConditions) {
							scanOperator = new SelectOperator(scanOperator, condition, schema);
						}
					}


					if (currentOperator == null) {
						currentOperator = scanOperator;
						combinedSchema = schema;
					} else {
						combinedSchema = Schema.combine(combinedSchema, schema);
						Expression joinCondition = clauseProcessor.getJoinExpression();
						currentOperator = new JoinOperator(currentOperator, scanOperator, joinCondition, combinedSchema);
					}
				}

				// Assuming you have a list of TableInfo objects from earlier in your method
				List<TableInfo> tableInfos = extractTables(plainSelect, catalog, aliasHandler);


				// Check for GROUP BY and SUM aggregation
				List<String> groupByColumnName = findGroupByColumnNames(plainSelect, combinedSchema); // This method should find and return the group by column name, or null if not present
				String sumColumnName = findSumColumnName(plainSelect); // This method should find and return the sum column name, or null if not present

				System.out.println("group by name: " + groupByColumnName);
				System.out.println("sum column name: " + sumColumnName);



				if (sumColumnName != null || !groupByColumnName.isEmpty()) {
					// Apply SumOperator

					SumOperator sumOperator = new SumOperator(currentOperator, sumColumnName, groupByColumnName, combinedSchema);

					combinedSchema = sumOperator.getSchema();
					combinedSchema.printSchema();
					currentOperator = sumOperator;
				}




				// Apply projection, considering aliases
				List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
				List<String> projectionColumns = parseSelectItems(selectItems, tableInfos); // Adjusted to use TableInfo

				boolean isSelectAllColumns = selectItems.stream().anyMatch(item -> item.toString().equals("*"));

				if (!isSelectAllColumns) {

					currentOperator = new ProjectOperator(currentOperator, combinedSchema, selectItems);

				}




				// Handle DISTINCT
				if (plainSelect.getDistinct() != null) {
					currentOperator = new DuplicateEliminationOperator(currentOperator, combinedSchema, projectionColumns);
				}






				//Order By
				List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
				if (orderByElements != null && !orderByElements.isEmpty()) {
					Comparator<Tuple> compositeComparator = null;

					for (OrderByElement orderByElement : orderByElements) {
						Column column = (Column) orderByElement.getExpression();
						int columnIndex = combinedSchema.getColumnIndex(column.getFullyQualifiedName());
						TupleComparator currentComparator = new TupleComparator(columnIndex);

						if (orderByElement.isAsc()) {
							compositeComparator = (compositeComparator == null) ? currentComparator
									: compositeComparator.thenComparing(currentComparator);
						} else {
							compositeComparator = (compositeComparator == null) ? currentComparator.reversed()
									: compositeComparator.thenComparing(currentComparator.reversed());
						}
					}

					// Assume SortOperator accepts a Comparator<Tuple> to sort the tuples accordingly
					currentOperator = new SortOperator(currentOperator, compositeComparator);
				}


				// Assuming dump method outputs the tuples
				currentOperator.dump(System.out);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Integer findGroupByColumnIndex(PlainSelect plainSelect, Schema schema) {
		GroupByElement groupBy = plainSelect.getGroupBy();
		if (groupBy != null) {
			List<Expression> groupByExpressions = groupBy.getGroupByExpressions();
			if (groupByExpressions != null && !groupByExpressions.isEmpty()) {
				// Assuming only the first column is used for GROUP BY
				Expression firstGroupByExpr = groupByExpressions.get(0);
				if (firstGroupByExpr instanceof Column) {
					String columnName = ((Column) firstGroupByExpr).getColumnName();
					// Assuming schema can resolve the column index by its name
					return schema.getColumnIndex(columnName);
				}
			}
		}
		return null; // No group by column found
	}

	private static List<String> findGroupByColumnNames(PlainSelect plainSelect, Schema schema) {
		List<String> groupByColumnNames = new ArrayList<>();
		GroupByElement groupBy = plainSelect.getGroupBy();
		if (groupBy != null) {
			List<Expression> groupByExpressions = groupBy.getGroupByExpressions();
			if (groupByExpressions != null && !groupByExpressions.isEmpty()) {
				for (Expression groupByExpr : groupByExpressions) {
					if (groupByExpr instanceof Column) {
						String columnName = ((Column) groupByExpr).getFullyQualifiedName();
						groupByColumnNames.add(columnName);
					}
				}
			}
		}
		return groupByColumnNames; // Return the list of GROUP BY column names
	}

	private static String findSumColumnName(PlainSelect plainSelect) {
		List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
		for (SelectItem item : selectItems) {
			String itemStr = item.toString();
			if (itemStr.toUpperCase().startsWith("SUM(")) {
				// Extracting column name from the SUM function
				int startIdx = itemStr.indexOf('(') + 1;
				int endIdx = itemStr.lastIndexOf(')');
				if (startIdx > 0 && endIdx > startIdx) {
					String columnName = itemStr.substring(startIdx, endIdx);
					// Assuming the column name extraction logic needs to be refined based on actual query structure
					return columnName;
				}
			}
		}
		return null; // No SUM function found
	}











//	public static List<Table> extractTables(PlainSelect plainSelect, DatabaseCatalog catalog, AliasHandler aliasHandler) {
//		List<Table> tables = new ArrayList<>();
//
//		// Extract the main table from the FROM clause
//		FromItem fromItem = plainSelect.getFromItem();
//		if (fromItem instanceof Table) {
//			tables.add((Table) fromItem);
//		}
//
//		// Extract tables from JOIN clauses, if any
//		if (plainSelect.getJoins() != null) {
//			for (Join join : plainSelect.getJoins()) {
//				FromItem joinItem = join.getRightItem();
//				if (joinItem instanceof Table) {
//					tables.add((Table) joinItem);
//				}
//			}
//		}
//		return tables;
//	}

	public static List<TableInfo> extractTables(PlainSelect plainSelect, DatabaseCatalog catalog, AliasHandler aliasHandler) {
		List<TableInfo> tables = new ArrayList<>();

		// Extract the main table from the FROM clause
		FromItem fromItem = plainSelect.getFromItem();
		if (fromItem instanceof Table) {
			Table table = (Table) fromItem;
			tables.add(new TableInfo(table.getName(), table.getAlias() != null ? table.getAlias().getName() : null));
		}

		// Extract tables from JOIN clauses, if any
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				FromItem joinItem = join.getRightItem();
				if (joinItem instanceof Table) {
					Table joinTable = (Table) joinItem;
					tables.add(new TableInfo(joinTable.getName(), joinTable.getAlias() != null ? joinTable.getAlias().getName() : null));
				}
			}
		}

		return tables;
	}





	private static void processTable(FromItem fromItem, DatabaseCatalog catalog, AliasHandler aliasHandler, Map<String, Operator> tableScanOperators, Map<String, Schema> tableSchemas) {
		if (fromItem instanceof Table) {
			Table table = (Table) fromItem;
			String tableName = aliasHandler.resolveTableName(table.getName());
			List<String> columnNames = catalog.getSchema(tableName);
			Schema schema = new Schema(tableName, columnNames);
			Operator scanOperator = new ScanOperator(tableName);


			tableScanOperators.put(tableName, scanOperator);
			tableSchemas.put(tableName, schema);
		}
	}








	private static List<String> parseSelectItems(List<SelectItem<?>> selectItems, List<TableInfo> tableInfos) {
		// Build a reverse mapping from table names and aliases to TableInfo objects for easy lookup.
		Map<String, TableInfo> reverseMapping = new HashMap<>();
		for (TableInfo tableInfo : tableInfos) {
			reverseMapping.put(tableInfo.getTableName(), tableInfo);
			// Also map the alias for direct access, ensuring alias is considered
			if (tableInfo.getAlias() != null) {
				reverseMapping.put(tableInfo.getAlias(), tableInfo);
			}
		}

		List<String> columns = new ArrayList<>();
		for (SelectItem item : selectItems) {

			Expression expression = item.getExpression();
			if (expression instanceof Column) {
				Column column = (Column) expression;
				String columnName = column.getColumnName();
				String tableNameOrAlias = column.getTable() != null ? column.getTable().getName() : null;

				// Directly use the table name or alias as found without resolving to the original table name
				// This approach maintains the alias (if used in the query) in the fully qualified column name
				String fullyQualifiedName = tableNameOrAlias != null ? tableNameOrAlias + "." + columnName : columnName;
				columns.add(fullyQualifiedName);
			}

		}
		return columns;
	}


}



//	public static void executeSQL(String databaseDir, String inputFile, String outputFile) {
//		try {
//			DatabaseCatalog catalog = DatabaseCatalog.getInstance();
//			catalog.initializeCatalog(databaseDir, databaseDir + "/schema.txt");
//
//			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
//			if (statement instanceof Select) {
//				Select select = (Select) statement;
//				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
//
//				// Handling the FROM item
//				Table leftTable = (Table) plainSelect.getFromItem();
//				String leftTableName = leftTable.getName();
//				List<String> leftColumnNames = catalog.getSchema(leftTableName);
//				Schema leftSchema = new Schema(leftTableName, leftColumnNames);
//				Operator finalOperator = new ScanOperator(leftTableName);
//
//				Schema combinedSchema = leftSchema; // Initialize combinedSchema with leftSchema
//
//				// Apply WHERE condition if present
//				Expression whereCondition = plainSelect.getWhere();
//				if (whereCondition != null) {
//					finalOperator = new SelectOperator(finalOperator, whereCondition, leftSchema);
//				}
//
//				// Handling JOINs
//				if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
//					for (Join join : plainSelect.getJoins()) {
//						Table rightTable = (Table) join.getRightItem();
//						String rightTableName = rightTable.getName();
//						List<String> rightColumnNames = catalog.getSchema(rightTableName);
//						Schema rightSchema = new Schema(rightTableName, rightColumnNames);
//						Operator rightScanOperator = new ScanOperator(rightTableName);
//
//
//						combinedSchema = leftSchema.combineWith(rightSchema); // Update combinedSchema
//
//						Expression joinCondition = join.getOnExpression();
//						finalOperator = new JoinOperator(finalOperator, rightScanOperator, joinCondition, combinedSchema);
//					}
//				}
//
//				// Handle projection
//				List<SelectItem> selectItems = plainSelect.getSelectItems();
//				if (!isSelectAll(selectItems)) {
//					List<String> projectionColumns = parseSelectItems(selectItems);
//					finalOperator = new ProjectOperator(finalOperator, combinedSchema, projectionColumns); // Use combinedSchema here
//				}
//
//				// Assuming dump method outputs the tuples
//				finalOperator.dump(System.out);
//			}
//		} catch (Exception e) {
//			System.err.println("Exception occurred during query execution");
//			e.printStackTrace();
//		}
//	}
