package ed.inf.adbs.lightdb;

import java.util.*;

public class Schema {
    private Map<String, Integer> columnMapping;


    // Updated constructor to initialize tableName
    public Schema(String tableName, List<String> columnNames) {
        columnMapping = new HashMap<>(); // Initialize for this instance
        for (int i = 0; i < columnNames.size(); i++) {
            String fullyQualifiedColumnName = tableName + "." + columnNames.get(i);
            columnMapping.put(fullyQualifiedColumnName, i);
        }
    }

    public Integer getColumnIndex(String columnName) {
        // Attempt to directly retrieve the column index using the full column name.
        if (columnMapping.containsKey(columnName)) {
            return columnMapping.get(columnName);
        }

        String simpleColumnName = columnName.substring(columnName.lastIndexOf('.') + 1);

        return columnMapping.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith("." + simpleColumnName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null); // Return null if no matching column is found.
    }



    public static Schema combine(Schema firstSchema, Schema secondSchema) {
        Map<String, Integer> combinedColumnMapping = new LinkedHashMap<>(firstSchema.columnMapping);
        int offset = firstSchema.columnMapping.size();
        secondSchema.columnMapping.forEach((columnName, index) -> combinedColumnMapping.put(columnName, index + offset));

        // This uses the primary constructor with an empty tableName as combined schemas don't have a single tableName.
        Schema combinedSchema = new Schema("", new ArrayList<>(combinedColumnMapping.keySet()));
        combinedSchema.columnMapping = combinedColumnMapping; // Directly setting the combined mapping
        return combinedSchema;
    }



    public void printSchema() {
        System.out.println("Schema:");
        columnMapping.forEach((key, value) -> System.out.println("Column: " + key + ", Index: " + value));
    }

}
