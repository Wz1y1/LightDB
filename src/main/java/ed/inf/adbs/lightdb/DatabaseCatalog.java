package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseCatalog {
    private static DatabaseCatalog instance = null;
    private final Map<String, String> tablePaths;
    private final Map<String, List<String>> schemas;

    private DatabaseCatalog() {
        tablePaths = new HashMap<>();
        schemas = new HashMap<>();
    }

    public static DatabaseCatalog getInstance() {
        if (instance == null) {
            instance = new DatabaseCatalog();
        }
        return instance;
    }

    public void addTable(String tableName, String tablePath) {
        tablePaths.put(tableName, tablePath);
    }

    public String getTablePath(String tableName) {
        return tablePaths.get(tableName);
    }

    public void addSchema(String tableName, List<String> schema) {
        schemas.put(tableName, schema);
    }

    public List<String> getSchema(String tableName) {
        return schemas.get(tableName);
    }

    // New method for initializing the catalog from a schema file
    public void initializeCatalog(String databaseDir, String schemaFilePath) throws IOException {
        Path path = Paths.get(schemaFilePath);
        List<String> lines = Files.readAllLines(path);

        for (String line : lines) {
            String[] tokens = line.split(" ");
            String tableName = tokens[0];
            List<String> columns = Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length));

            String tablePath = databaseDir + "/data/" + tableName + ".csv";
            addTable(tableName, tablePath);
            addSchema(tableName, columns);
        }
    }
}
