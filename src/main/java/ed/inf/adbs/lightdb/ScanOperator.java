package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * The {@code ScanOperator} class extends {@code Operator} to implement a table scan.
 * It reads tuples from a specified table's file, one line at a time, converting each line
 * into a {@link Tuple} object.
 */
public class ScanOperator extends Operator {
    private BufferedReader reader;
    private final String tableName;

    /**
     * Constructs a new {@code ScanOperator} for the specified table.
     * Initializes a {@link BufferedReader} to read from the table's data file.
     *
     * @param tableName The name of the table to scan.
     */
    public ScanOperator(String tableName) {
        this.tableName = tableName;
        try {
            Path filePath = DatabaseCatalog.getInstance().getTablePath(tableName);
            reader = Files.newBufferedReader(filePath);
        } catch (IOException e) {
            System.err.println("Failed to open or find file for table: " + tableName);
            e.printStackTrace();
        }
    }

    /**
     * Retrieves the next tuple from the table's file.
     * Reads a line from the file, parses it into integer values, and wraps it in a {@link Tuple} object.
     *
     * @return The next {@link Tuple} in the table, or {@code null} if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                List<Integer> values = parseLineToIntegers(line);
                return new Tuple(values);
            }
        } catch (IOException e) {
            System.err.println("Error reading from file for table: " + tableName);
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Error parsing integer value in table: " + tableName);
            e.printStackTrace();
        }
        return null;
    }


    /**
     * Parses a line of text into a list of integers.
     * Assumes that values in the line are separated by commas.
     *
     * @param line The line of text to parse.
     * @return A list of integer values extracted from the line.
     */
    private List<Integer> parseLineToIntegers(String line) {
        return Arrays.stream(line.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }


    /**
     * Resets the operator to the beginning of the table's file.
     * Closes the current reader and re-initializes it to read from the start of the file.
     */
    @Override
    public void reset() {
        try {
            if (reader != null) {
                reader.close();
            }
            Path filePath = DatabaseCatalog.getInstance().getTablePath(tableName);
            reader = Files.newBufferedReader(filePath);
        } catch (IOException e) {
            System.err.println("Failed to reset reader for table: " + tableName);
            e.printStackTrace();
        }
    }
}
