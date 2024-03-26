//package ed.inf.adbs.lightdb;
//
//import net.sf.jsqlparser.schema.Table;
//import net.sf.jsqlparser.statement.select.*;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class AliasHandler {
//    // Maps table aliases to their actual table names
//    private Map<String, String> aliasToTableMap = new HashMap<>();
//
//    public AliasHandler(PlainSelect plainSelect) {
//        processFromItem(plainSelect.getFromItem());
//        if (plainSelect.getJoins() != null) {
//            plainSelect.getJoins().forEach(this::processJoin);
//        }
//    }
//
//    private void processFromItem(FromItem fromItem) {
//        if (fromItem instanceof Table) {
//            Table table = (Table) fromItem;
//            String tableName = table.getName();
//            if (table.getAlias() != null && table.getAlias().getName() != null) {
//                aliasToTableMap.put(table.getAlias().getName(), tableName);
//            } else {
//                // When no alias is used, map the table name to itself for direct references
//                aliasToTableMap.put(tableName, tableName);
//            }
//        }
//    }
//
//    private void processJoin(Join join) {
//        FromItem rightItem = join.getRightItem();
//        processFromItem(rightItem);
//    }
//
//    public String resolveTableName(String nameOrAlias) {
//        return aliasToTableMap.getOrDefault(nameOrAlias, nameOrAlias);
//    }
//}
