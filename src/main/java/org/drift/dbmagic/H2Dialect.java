package org.drift.dbmagic;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Dima Frid
 */
public class H2Dialect extends Dialect {

    private static final EnumMap<ColumnType, String> TYPE_MAP = new EnumMap<ColumnType, String>(ColumnType.class);

    static {
        TYPE_MAP.put(ColumnType.BOOLEAN, "TINYINT"); // There is a BOOLEAN type, but it returns java.lang.Boolean which we are not used to
        TYPE_MAP.put(ColumnType.INTEGER, "INT");
        TYPE_MAP.put(ColumnType.LONG, "BIGINT");
        TYPE_MAP.put(ColumnType.FLOAT, "FLOAT");
        TYPE_MAP.put(ColumnType.DOUBLE, "DOUBLE");
        TYPE_MAP.put(ColumnType.DATE, "DATE");
        TYPE_MAP.put(ColumnType.VARBINARY, "RAW");
        TYPE_MAP.put(ColumnType.VARCHAR, "VARCHAR");
        TYPE_MAP.put(ColumnType.BLOB, "BLOB");
        TYPE_MAP.put(ColumnType.CLOB, "CLOB");
        TYPE_MAP.put(ColumnType.TIMESTAMP, "TIMESTAMP");
        TYPE_MAP.put(ColumnType.ID, "BIGINT");
        TYPE_MAP.put(ColumnType.UUID_STR, "VARCHAR");
    }

    protected EnumMap<ColumnType, String> getTypeMappings() {
        return TYPE_MAP;
    }

    @Override
    public String tableExistenceQuery() {
        return "select 1 from information_schema.tables where table_name = ?";
    }

    @Override
    public String indexExistenceQuery() {
        return "select 1 from information_schema.indexes where index_name = ? and table_name = ? ";
    }

    @Override
    public Map<String, ColumnDescription> getColumns(String tableName, JdbcTemplate template) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPrimaryKey(String tableName, JdbcTemplate template) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getIndexes(String tableName, JdbcTemplate template) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean pkRequiresIndex() {
        return true;
    }

}