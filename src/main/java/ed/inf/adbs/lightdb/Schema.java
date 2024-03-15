package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//
//public class Schema {
//    private final Map<String, Integer> columnMapping;
//
//
//    // Constructor now takes tableName as an additional parameter
//    public Schema(String tableName, List<String> columnNames) {
//        columnMapping = new HashMap<>();
//        // Prefix each column name with the table name
//        for (int i = 0; i < columnNames.size(); i++) {
//            String fullyQualifiedColumnName = tableName + "." + columnNames.get(i);
//            columnMapping.put(fullyQualifiedColumnName, i);
//        }
//    }
//
//    public Integer getColumnIndex(String fullyQualifiedColumnName) {
//        return columnMapping.get(fullyQualifiedColumnName);
//    }
//}
public class Schema {
    private Map<String, Integer> columnMapping;
    private final String tableName; // Keep track of the tableName

    // Updated constructor to initialize tableName
    public Schema(String tableName, List<String> columnNames) {
        this.tableName = tableName;
        columnMapping = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String fullyQualifiedColumnName = tableName + "." + columnNames.get(i);
            columnMapping.put(fullyQualifiedColumnName, i);
        }
    }

    public Integer getColumnIndex(String fullyQualifiedColumnName) {
        return columnMapping.get(fullyQualifiedColumnName);
    }

    // Method to combine this schema with another schema
    public Schema combineWith(Schema other) {
        // Extract column names from both schemas and combine them
        List<String> combinedColumnNames = new ArrayList<>();
        for (String key : this.columnMapping.keySet()) {
            combinedColumnNames.add(key);
        }
        for (String key : other.columnMapping.keySet()) {
            combinedColumnNames.add(key);
        }

        // Create a new combined Schema without directly setting columnMapping
        // Assuming you might adjust your Schema constructor to accept a list of fully qualified column names
        return new Schema("Combined", combinedColumnNames);
    }


    // Additional methods...
}
