package ed.inf.adbs.lightdb;

public class TableInfo {
    private String tableName;
    private String alias;

    public TableInfo(String tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias != null ? alias : tableName; // Return alias if present; otherwise, table name.
    }
}
