package ed.inf.adbs.lightdb;


/**
 * Represents information about a table within a database, including its name and potentially an alias.
 * This class is used to encapsulate the details of a table, making it easier to manage table references,
 * especially in contexts where tables may be referred to by aliases.
 */
public class TableInfo {
    private final String tableName;
    private final String alias;


    /**
     * Constructs a new {@code TableInfo} instance with the specified table name and alias.
     *
     * @param tableName The name of the table. This parameter cannot be {@code null}.
     * @param alias The alias of the table. This can be {@code null} if the table does not have an alias.
     */
    public TableInfo(String tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
    }


    /**
     * Returns the name of the table.
     *
     * @return The table name as a {@code String}. This value cannot be {@code null}.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the alias of the table if one is present; otherwise, returns the table name.
     * This method ensures that there is always a valid string to refer to the table, whether it is
     * by its original name or an alias.
     *
     * @return The alias of the table as a {@code String} if an alias is present; otherwise, the table name.
     */
    public String getAlias() {
        return alias != null ? alias : tableName;
    }
}
