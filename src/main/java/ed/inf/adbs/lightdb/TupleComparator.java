package ed.inf.adbs.lightdb;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {
    private final int columnIndex;

    public TupleComparator(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        Comparable<Integer> value1 = t1.getValues().get(columnIndex);
        Integer value2 = t2.getValues().get(columnIndex);
        return value1.compareTo(value2);
    }
}

