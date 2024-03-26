package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * The {@code DatabaseCatalog} class serves as a singleton repository for database metadata,
 * including the paths to table data files and the schema definitions of tables within a database.
 * It provides methods to add and retrieve table paths and schemas, and to initialize the catalog
 * from a schema definition file.
 *
 * <p> This class employs the Initialization-on-demand holder idiom for its singleton implementation,
 * ensuring thread-safe lazy initialization of the singleton instance.
 */
public class DatabaseCatalog {
    private final Map<String, Path> tablePaths = new HashMap<>();
    private final Map<String, List<String>> schemas = new HashMap<>();

    // Private constructor to prevent direct instantiation.
    private DatabaseCatalog() {}

    /**
     * Holder class for the {@code DatabaseCatalog} singleton instance.
     * Utilizes the Initialization-on-demand holder idiom.
     */
    private static class Holder {
        static final DatabaseCatalog INSTANCE = new DatabaseCatalog();
    }

    /**
     * Returns the singleton instance of {@code DatabaseCatalog}.
     *
     * @return the singleton {@code DatabaseCatalog} instance.
     */
    public static DatabaseCatalog getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Adds a table and its corresponding file path to the catalog.
     *
     * @param tableName The name of the table to add.
     * @param tablePath The file path where the table's data is stored.
     */
    public void addTable(String tableName, Path tablePath) {
        tablePaths.put(tableName, tablePath);
    }

    /**
     * Retrieves the file path associated with a given table.
     *
     * @param tableName The name of the table whose path is being retrieved.
     * @return The file path of the specified table, or {@code null} if the table does not exist.
     */
    public Path getTablePath(String tableName) {
        return tablePaths.get(tableName);
    }

    /**
     * Adds a schema definition for a table to the catalog.
     *
     * @param tableName The name of the table whose schema is being added.
     * @param schema A list of column names defining the schema of the table.
     */
    public void addSchema(String tableName, List<String> schema) {
        schemas.put(tableName, schema);
    }


    /**
     * Retrieves the schema of a specified table.
     *
     * @param tableName The name of the table whose schema is being retrieved.
     * @return A list of column names constituting the schema of the specified table, or {@code null} if the table does not exist.
     */
    public List<String> getSchema(String tableName) {
        return schemas.get(tableName);
    }

    /**
     * Initializes the catalog by loading table paths and schemas from a schema definition file.
     * Each line in the file should define a table and its columns, formatted as "tableName columnName1 columnName2 ...".
     *
     * @param databaseDir The directory path of the database where table data files are stored.
     * @param schemaFilePath The path to the schema definition file.
     * @throws IOException if an I/O error occurs reading from the file or a malformed or unmappable byte sequence is read.
     */
    public void initializeCatalog(String databaseDir, String schemaFilePath) throws IOException {
        Path schemaPath = Paths.get(schemaFilePath);
        List<String> lines = Files.readAllLines(schemaPath);
        lines.forEach(line -> parseAndAddTableSchema(line, Paths.get(databaseDir, "data")));
    }

    /**
     * Parses a line from the schema definition file and adds the table schema and path to the catalog.
     *
     * @param line A line from the schema definition file, representing a table and its columns.
     * @param dataDir The directory where table data files are stored.
     */
    private void parseAndAddTableSchema(String line, Path dataDir) {
        String[] tokens = line.trim().split("\\s+");
        if (tokens.length < 2) {
            // Handle error or log warning
            System.err.println("Invalid schema definition: " + line);
            return;
        }
        String tableName = tokens[0];
        List<String> columns = Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length));

        Path tablePath = dataDir.resolve(tableName + ".csv");
        addTable(tableName, tablePath);
        addSchema(tableName, columns);
    }
}
