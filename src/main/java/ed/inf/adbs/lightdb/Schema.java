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

    private Schema(Map<String, Integer> combinedMapping) {
        this.columnMapping = combinedMapping;
    }


    public Integer getColumnIndex(String fullyQualifiedColumnName) {
        return columnMapping.get(fullyQualifiedColumnName);
    }

    // Add a method to get all column names
    public List<String> getColumnNames() {
        return new ArrayList<>(columnMapping.keySet());
    }

    // Method to combine this schema with another schema
    public Schema combineWith(Schema otherSchema) {
        Map<String, Integer> combinedColumnMapping = new LinkedHashMap<>(this.columnMapping);
        int offset = this.columnMapping.size();
        otherSchema.columnMapping.forEach((columnName, index) -> combinedColumnMapping.put(columnName, index + offset));

        // Using a more descriptive combined table name
        return new Schema(combinedColumnMapping);
    }

    // Or a setter method
    public void setColumnMapping(Map<String, Integer> columnMapping) {
        this.columnMapping = columnMapping;
    }

    public void printSchema() {
        System.out.println("Schema:");
        columnMapping.forEach((key, value) -> System.out.println("Column: " + key + ", Index: " + value));
    }



    // Additional methods...
}
