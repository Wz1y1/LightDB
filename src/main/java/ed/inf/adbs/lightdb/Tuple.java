package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a tuple in a database table, encapsulating a list of integer values that correspond to the columns of the table.
 */
public class Tuple {
    private final List<Integer> values;

    /**
     * Constructs a new Tuple with the specified values.
     *
     * @param values A list of integers representing the values of the tuple.
     */
    public Tuple(List<Integer> values) {
        this.values = new ArrayList<>(values);
    }

    /**
     * Returns a new list containing the values of this tuple.
     *
     * @return A new list of integers representing the tuple's values.
     */
    public List<Integer> getValues() {
        return new ArrayList<>(values);
    }

    /**
     * Retrieves the value at the specified index in this tuple.
     *
     * @param index The index of the value to retrieve.
     * @return The integer value at the specified index.
     */
    public int getValueByPos(int index) {
        return values.get(index);
    }

    /**
     * Returns the size of this tuple, which corresponds to the number of elements (columns) it contains.
     *
     * @return The size (number of elements) of the tuple.
     */
    public int getSize() {
        return values.size();
    }


    /**
     * Returns a string representation of the tuple, with values separated by commas.
     *
     * @return A string representation of the tuple.
     */
    @Override
    public String toString() {
        return values.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
    }

    /**
     * Compares this tuple to another tuple based on the value at a specified column index.
     *
     * @param other The tuple to compare this tuple to.
     * @param columnIndex The index of the column to use for the comparison.
     * @return A negative integer, zero, or a positive integer as this tuple's value at the specified column index
     *         is less than, equal to, or greater than the specified tuple's value at the same column index.
     * @throws IllegalArgumentException If the other tuple is null, the columnIndex is out of range, or if the
     *                                  columnIndex is invalid for either tuple.
     */
    public int compareTo(Tuple other, int columnIndex) {
        if (other == null || columnIndex < 0 || columnIndex >= this.values.size() || columnIndex >= other.values.size()) {
            throw new IllegalArgumentException("Invalid comparison argument(s)");
        }
        return Integer.compare(this.values.get(columnIndex), other.values.get(columnIndex));
    }
}
