package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ScanOperator extends Operator {
    private BufferedReader reader;
    private final String tableName;

    public ScanOperator(String tableName) {
        this.tableName = tableName;
        try {
            String filePath = DatabaseCatalog.getInstance().getTablePath(tableName);
            reader = Files.newBufferedReader(Paths.get(filePath));
        } catch (IOException e) {
            System.err.println("Failed to open or find file for table: " + tableName);
            e.printStackTrace();
        }
    }

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


    private List<Integer> parseLineToIntegers(String line) {
        return Arrays.stream(line.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    @Override
    public void reset() {
        try {
            if (reader != null) {
                reader.close();
            }
            String filePath = DatabaseCatalog.getInstance().getTablePath(tableName);
            reader = Files.newBufferedReader(Paths.get(filePath));
        } catch (IOException e) {
            System.err.println("Failed to reset reader for table: " + tableName);
            e.printStackTrace();
        }
    }
}
