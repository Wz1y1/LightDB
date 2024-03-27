package ed.inf.adbs.lightdb;

import java.io.*;

import java.util.*;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
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


	/**
	 * Executes an SQL query against a database.
	 *
	 * @param databaseDir Directory where the database is located.
	 * @param inputFile Path to the SQL query file.
	 * @param outputFile Path to the output file for the query results.
	 */
	public static void executeSQL(String databaseDir, String inputFile, String outputFile) {
		try {
			// Initialize the catalog with schema information
			DatabaseCatalog catalog = DatabaseCatalog.getInstance();
			catalog.initializeCatalog(databaseDir, databaseDir + "/schema.txt");

			// Parse the SQL query
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));

			if (statement instanceof Select) {
				Select select = (Select) statement;
				PlainSelect plainSelect = (PlainSelect) select;

				Operator rootOperator = null;
				Schema combinedSchema = null;

				// Process WHERE clause to identify table-specific and join conditions
				WhereClauseProcessor clauseProcessor = new WhereClauseProcessor();
				if (plainSelect.getWhere() != null) {
					plainSelect.getWhere().accept(clauseProcessor);
				}

				// Extracts table information, including names and aliases
				List<TableInfo> tableInfos = extractTables(plainSelect);
				// Iterate over tables in the FROM clause, applying SCAN/SELECT and JOIN operators

				// For each table (or alias), process schema resolution, condition application, and operator combination.
				for (TableInfo tableInfo : tableInfos) {
					String tableName = tableInfo.getTableName();
					String aliasName = tableInfo.getAlias();
					String resolvedTableName = aliasName != null ? aliasName : tableName;

					// Step 1: New Schema using the Alias if present
					Schema schema = resolveTableSchema(catalog, tableName, resolvedTableName);

					// Step 2: Initialize Scan Operator and Apply Conditions
					Operator scanOperator = initializeScanOperatorAndApplyConditions(clauseProcessor,tableName, schema, resolvedTableName);

					if (rootOperator == null) {
						rootOperator = scanOperator;
						combinedSchema = schema;
					} else {
						combinedSchema = Schema.combine(combinedSchema, schema);
						List<Expression> joinCondition = clauseProcessor.getJoinExpressions();

						// Check if the join condition can be applied (i.e., all columns involved are present in the combinedSchema)
						boolean canApplyJoinCondition = canApplyJoinCondition(joinCondition, combinedSchema);
						rootOperator = new JoinOperator(rootOperator, scanOperator, canApplyJoinCondition ? joinCondition : null, combinedSchema);
					}
				}

				// Aggregates data if GROUP BY or SUM is specified in the query.
				List<String> groupByColumnName = findGroupByColumnNames(plainSelect); // This method should find and return the group by column name, or null if not present
				String sumColumnName = findSumColumnName(plainSelect); // This method should find and return the sum column name, or null if not present
				if (sumColumnName != null || !groupByColumnName.isEmpty()) {
					// Apply SumOperator
					SumOperator sumOperator = new SumOperator(rootOperator, sumColumnName, groupByColumnName, combinedSchema);
					combinedSchema = sumOperator.getSchema();
					rootOperator = sumOperator;
				}


				// Applies PROJECTION to include only the specified columns in the final result set.
				List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
				List<String> projectionColumns = parseSelectItems(selectItems, tableInfos);
				boolean selectAll = selectItems.stream().anyMatch(item -> item.toString().equals("*"));
				if (!selectAll) {
					// Filters the columns based on the current operator.
					rootOperator = new ProjectOperator(rootOperator, combinedSchema, selectItems);
				}


				// Eliminates duplicate rows if DISTINCT is specified.
				if (plainSelect.getDistinct() != null) {
					assert combinedSchema != null;
					rootOperator = new DuplicateEliminationOperator(rootOperator, combinedSchema, projectionColumns);
				}


				// Extracts the ORDER BY elements from the SQL query
				List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
				// Checks if there are any ORDER BY elements to apply.
				if (orderByElements != null && !orderByElements.isEmpty()) {
					Comparator<Tuple> compositeComparator = null;

					// Iterates over each ORDER BY element to build the composite comparator.
					for (OrderByElement orderByElement : orderByElements) {
						Column column = (Column) orderByElement.getExpression();
						assert combinedSchema != null;
						int columnIndex = combinedSchema.getIndex(column.getFullyQualifiedName());

						// Creates a comparator for the current column, comparing tuples based on the column's value.
						Comparator<Tuple> currentComparator = (t1, t2) -> t1.compareTo(t2, columnIndex);
						// If the ORDER BY clause specifies descending order, reverse the comparator.
						if (!orderByElement.isAsc()) {
							currentComparator = currentComparator.reversed();
						}

						// Combines the current comparator with the composite comparator to handle multiple ORDER BY criteria.
						compositeComparator = (compositeComparator == null) ? currentComparator
								: compositeComparator.thenComparing(currentComparator);
					}
					rootOperator = new SortOperator(rootOperator, compositeComparator);
				}

//				assert rootOperator != null;
//				rootOperator.dump(System.out);

				// Export the content to the file
				try (PrintStream out = new PrintStream(new FileOutputStream(outputFile))) {
					assert rootOperator != null;
					rootOperator.dump(out);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	/**
	 * Resolves the schema for a given table by retrieving its column names from the database catalog
	 * and creates a new Schema instance.
	 *
	 * @param catalog The database catalog containing schema information.
	 * @param tableName The name of the table for which to resolve the schema.
	 * @param resolvedTableName The name to be used in the schema, allowing for aliasing.
	 * @return A Schema object representing the structure of the specified table.
	 */
	private static  Schema resolveTableSchema(DatabaseCatalog catalog, String tableName, String resolvedTableName) {
		List<String> columnNames = catalog.getSchema(tableName);
		return new Schema(resolvedTableName, columnNames);
	}


	/**
	 * Initializes a scan operator for a specified table and applies any table-specific conditions
	 * found in the WHERE clause. Conditions are processed to filter tuples according to the criteria.
	 *
	 * @param clauseProcessor The processor handling WHERE clause logic, extracting conditions specific to tables.
	 * @param tableName The name of the table to scan.
	 * @param schema The schema of the table being scanned, used to resolve column references in conditions.
	 * @param resolvedTableName The resolved table name, accounting for potential aliasing.
	 * @return An Operator that represents the scan operation possibly wrapped with select operators for each condition.
	 */
	private static Operator initializeScanOperatorAndApplyConditions(WhereClauseProcessor clauseProcessor,String tableName, Schema schema, String resolvedTableName) {
		Operator scanOperator = new ScanOperator(tableName);
		List<Expression> tableSpecificConditions = clauseProcessor.getConditionsForTable(resolvedTableName);
		if (tableSpecificConditions != null) {
			for (Expression condition : tableSpecificConditions) {
				scanOperator = new SelectOperator(scanOperator, condition, schema);
			}
		}
		return scanOperator;
	}


		/**
         * Checks if all columns involved in join conditions are present in the combined schema.
         * This method is essential for determining if join conditions are applicable based on the current schema.
         *
         * @param joinConditions The list of join conditions to evaluate.
         * @param combinedSchema The combined schema of all tables involved in the join.
         * @return True if all columns mentioned in the join conditions exist in the combined schema; otherwise, false.
         */
	private static boolean canApplyJoinCondition(List<Expression> joinConditions, Schema combinedSchema) {
		if (joinConditions == null || joinConditions.isEmpty()) return false;

		// Flatten all involved columns from all join conditions into a single list
		List<String> involvedColumns = joinConditions.stream()
				.flatMap(expr -> extractColumnNames(expr).stream())
				.collect(Collectors.toList());

		// Check if all involved columns are present in the combined schema
		return involvedColumns.stream().allMatch(combinedSchema::hasColumn);
	}

	/**
	 * Extracts column names from an expression, typically used to retrieve column names from conditions.
	 *
	 * @param expression The expression from which to extract column names.
	 * @return A list of fully qualified column names extracted from the expression.
	 */
	public static List<String> extractColumnNames(Expression expression) {
		List<String> columnNames = new ArrayList<>();
		expression.accept(new ExpressionVisitorAdapter() {
			@Override
			public void visit(Column column) {
				columnNames.add(column.getFullyQualifiedName());
			}

		});
		return columnNames;
	}

	/**
	 * Finds column names specified in the GROUP BY clause of the query.
	 *
	 * @param plainSelect The SELECT statement being processed.
	 * @return A list of column names used in the GROUP BY clause.
	 */
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

	/**
	 * Identifies the column name used in the SUM function of the SELECT statement, if present.
	 *
	 * @param plainSelect The SELECT statement being processed.
	 * @return The column name used in the SUM function, or null if no SUM function is present.
	 */
	private static String findSumColumnName(PlainSelect plainSelect) {
		for (SelectItem item : plainSelect.getSelectItems()) {
			String itemStr = item.toString();
			if (itemStr.toUpperCase().startsWith("SUM(")) {
				return itemStr.substring(itemStr.indexOf('(') + 1, itemStr.lastIndexOf(')'));
			}
		}
		return null;
	}



	/**
	 * Extracts tables, including aliases if present, from the FROM and JOIN clauses of the query.
	 *
	 * @param plainSelect The SELECT statement being processed.
	 * @return A list of {@link TableInfo} objects representing the tables involved in the query.
	 */
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

	/**
	 * Parses SELECT items to determine the columns to project.
	 * This method supports queries that use table aliases by maintaining the alias association.
	 *
	 * @param selectItems The SELECT items to parse.
	 * @param tableInfos  Information about tables and their aliases in the query.
	 * @return A list of strings representing the columns to include in the projection.
	 */
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
