//package ed.inf.adbs.lightdb;
//
//import java.util.Comparator;
//import java.util.List;
//
//public class SpecificColumnsComparator implements Comparator<Tuple> {
//    private final List<Integer> columnIndexes;
//
//    public SpecificColumnsComparator(List<Integer> columnIndexes) {
//        this.columnIndexes = columnIndexes;
//    }
//
//    @Override
//    public int compare(Tuple o1, Tuple o2) {
//        for (int index : columnIndexes) {
//            Comparable<Integer> value1 = o1.getValues().get(index);
//            Integer value2 = o2.getValues().get(index);
//            if (value1 == null ^ value2 == null) {
//                return (value1 == null) ? -1 : 1;
//            }
//            if (value1 != null) {
//                int result = value1.compareTo(value2);
//                if (result != 0) {
//                    return result;
//                }
//            }
//        }
//        return 0; // Tuples are equal or both values are null for specified columns
//    }
//}
//
