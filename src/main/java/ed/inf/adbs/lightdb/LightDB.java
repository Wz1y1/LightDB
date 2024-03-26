package ed.inf.adbs.lightdb;

import java.io.FileReader;

import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
				PlainSelect plainSelect = (PlainSelect) select;

				WhereClauseProcessor clauseProcessor = new WhereClauseProcessor();
				if (plainSelect.getWhere() != null) {
					plainSelect.getWhere().accept(clauseProcessor);
				}

				Operator currentOperator = null;
				Schema combinedSchema = null;



				// Iterate over tables, now considering aliases
				for (TableInfo tableInfo : extractTables(plainSelect)) {
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

						// Check if the join condition can be applied (i.e., all columns involved are present in the combinedSchema)
						boolean canApplyJoinCondition = canApplyJoinCondition(joinCondition, combinedSchema);


						currentOperator = new JoinOperator(currentOperator, scanOperator, canApplyJoinCondition ? joinCondition : null, combinedSchema);

					}
				}

				// Assuming you have a list of TableInfo objects from earlier in your method
				List<TableInfo> tableInfos = extractTables(plainSelect);


				// Check for GROUP BY and SUM aggregation
				List<String> groupByColumnName = findGroupByColumnNames(plainSelect); // This method should find and return the group by column name, or null if not present
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
						assert combinedSchema != null;
						int columnIndex = combinedSchema.getColumnIndex(column.getFullyQualifiedName());

						Comparator<Tuple> currentComparator = (t1, t2) -> t1.compareTo(t2, columnIndex);

						if (!orderByElement.isAsc()) {
							currentComparator = currentComparator.reversed();
						}

						compositeComparator = (compositeComparator == null) ? currentComparator
								: compositeComparator.thenComparing(currentComparator);
					}

					// Assuming SortOperator accepts a Comparator<Tuple> to sort the tuples accordingly
					currentOperator = new SortOperator(currentOperator, compositeComparator);
				}


				// Assuming dump method outputs the tuples
				assert currentOperator != null;
				currentOperator.dump(System.out);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static boolean canApplyJoinCondition(Expression joinCondition, Schema combinedSchema) {
		if (joinCondition == null) return false;

		// This utility method needs to parse the joinCondition to extract column names involved.
		List<String> involvedColumns = extractColumnNames(joinCondition);

		// Use combinedSchema.hasColumn to check if all columns are present.
		return involvedColumns.stream().allMatch(combinedSchema::hasColumn);
	}


	public static List<String> extractColumnNames(Expression joinCondition) {
		List<String> columnNames = new ArrayList<>();
		if (joinCondition instanceof EqualsTo) {
			EqualsTo equals = (EqualsTo) joinCondition;
			if (equals.getLeftExpression() instanceof Column && equals.getRightExpression() instanceof Column) {
				columnNames.add(((Column) equals.getLeftExpression()).getFullyQualifiedName());
				columnNames.add(((Column) equals.getRightExpression()).getFullyQualifiedName());
			}
		}
		return columnNames;
	}

	private static List<String> findGroupByColumnNames(PlainSelect plainSelect) {
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



	public static List<TableInfo> extractTables(PlainSelect plainSelect) {
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
