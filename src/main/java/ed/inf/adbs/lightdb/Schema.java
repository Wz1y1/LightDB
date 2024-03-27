package ed.inf.adbs.lightdb;

import java.util.*;

/**
 * Represents the schema of a table or a combination of tables in the database.
 * It maps column names to their index positions to facilitate column access in tuples.
 */
public class Schema {
    // Maps fully qualified column names to their index positions
    private final Map<String, Integer> columnIndex;

    /**
     * Initializes a schema with specified table name and its columns.
     *
     * @param tableName   The name of the table, used to qualify column names.
     * @param nameList The list of columns in the table.
     */
    public Schema(String tableName, List<String> nameList) {
        this.columnIndex = new LinkedHashMap<>();
        for (int i = 0; i < nameList.size(); i++) {
            // Qualify each column name with the table name and add it to the mapping.
            columnIndex.put(qualifyColumnName(tableName, nameList.get(i)), i);
        }
    }

    /**
     * Retrieves the index of a column given its name.
     *
     * @param columnName  The name of the column (may be fully qualified or simple).
     * @return The index of the column, or null if the column does not exist in the schema.
     */
    public int getIndex(String columnName) {
        if (columnIndex.containsKey(columnName)) {
            return columnIndex.get(columnName);
        }
        // If the column name is unqualified, try to match the last segment of the qualified names.
        for (String key : columnIndex.keySet()) {
            if (key.endsWith("." + columnName)) {
                return columnIndex.get(key);
            }
        }
        return -1; // Indicate column not found
    }

    /**
     * Checks if the schema contains a given column name.
     *
     * @param columnName The name of the column to check.
     * @return true if the schema contains the column, false otherwise.
     */
    public boolean hasColumn(String columnName) {
        return columnIndex.containsKey(columnName) ||
                columnIndex.keySet().stream().anyMatch(key -> key.endsWith("." + columnName));
    }

    /**
     * Merges two schemas into one, combining their column mappings.
     *
     * @param firstSchema  The first schema to merge.
     * @param secondSchema The second schema to merge.
     * @return A new Schema instance representing the combined schemas.
     */
    public static Schema combine(Schema firstSchema, Schema secondSchema) {
        Schema combinedSchema = new Schema("", new ArrayList<>()); // Initial combined schema without column names
        combinedSchema.columnIndex.putAll(firstSchema.columnIndex); // Add all from first schema
        secondSchema.columnIndex.forEach((columnName, index) ->
                combinedSchema.columnIndex.put(columnName, index + firstSchema.columnIndex.size())); // Offset and add from second schema
        return combinedSchema;
    }

    /**
     * Qualifies a column name with its table name if not already qualified.
     *
     * @param tableName  The name of the table.
     * @param columnName The column name to qualify.
     * @return The qualified column name.
     */
    private String qualifyColumnName(String tableName, String columnName) {
        return tableName.isEmpty() ? columnName : tableName + "." + columnName;
    }
}
