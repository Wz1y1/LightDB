package ed.inf.adbs.lightdb;

import java.util.*;

/**
 * Represents the schema of a table or a combination of tables in the database.
 * It maps column names to their index positions to facilitate column access in tuples.
 */
public class Schema {
    // Maps fully qualified column names to their index positions
    private Map<String, Integer> columnMapping;

    /**
     * Constructs a new Schema instance for a given table.
     *
     * @param tableName    The name of the table.
     * @param columnNames  A list of column names for the table.
     */
    public Schema(String tableName, List<String> columnNames) {
        columnMapping = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String fullyQualifiedColumnName = tableName + "." + columnNames.get(i);
            columnMapping.put(fullyQualifiedColumnName, i);
        }
    }

    /**
     * Retrieves the index of a column given its name.
     *
     * @param columnName  The name of the column (may be fully qualified or simple).
     * @return The index of the column, or null if the column does not exist in the schema.
     */
    public Integer getColumnIndex(String columnName) {
        // Attempt to directly retrieve the column index using the full column name.
        if (columnMapping.containsKey(columnName)) {
            return columnMapping.get(columnName);
        }
        // Handles unqualified column names by searching for a matching end segment
        return columnMapping.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith("." + columnName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null); // Return null if no matching column is found.
    }


    /**
     * Combines two Schemas into a single Schema, preserving column order.
     *
     * @param firstSchema  The first schema to combine.
     * @param secondSchema The second schema to combine.
     * @return A new Schema instance representing the combination of both input schemas.
     */
    public static Schema combine(Schema firstSchema, Schema secondSchema) {
        Map<String, Integer> combinedColumnMapping = new LinkedHashMap<>(firstSchema.columnMapping);
        int offset = firstSchema.columnMapping.size();
        secondSchema.columnMapping.forEach((columnName, index) -> combinedColumnMapping.put(columnName, index + offset));
        Schema combinedSchema = new Schema("", new ArrayList<>(combinedColumnMapping.keySet()));
        combinedSchema.columnMapping = combinedColumnMapping;
        return combinedSchema;
    }

    /**
     * Checks if the schema contains a given column name.
     *
     * @param columnName The name of the column to check.
     * @return true if the schema contains the column, false otherwise.
     */
    public boolean hasColumn(String columnName) {
        return columnMapping.containsKey(columnName) ||
                columnMapping.keySet().stream().anyMatch(key -> key.endsWith("." + columnName));
    }



    public void printSchema() {
        System.out.println("Schema:");
        columnMapping.forEach((key, value) -> System.out.println("Column: " + key + ", Index: " + value));
    }

}
