package org.drift.dbmagic;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Dima Frid
 */
public class OracleDialect extends Dialect {

    private static final EnumMap<ColumnType, String> TYPE_MAP = new EnumMap<ColumnType, String>(ColumnType.class);

    static {
        TYPE_MAP.put(ColumnType.BOOLEAN, "NUMBER");
        TYPE_MAP.put(ColumnType.INTEGER, "NUMBER");
        TYPE_MAP.put(ColumnType.LONG, "NUMBER");
        TYPE_MAP.put(ColumnType.FLOAT, "NUMBER");
        TYPE_MAP.put(ColumnType.DOUBLE, "NUMBER");
        TYPE_MAP.put(ColumnType.DATE, "DATE");
        TYPE_MAP.put(ColumnType.VARBINARY, "RAW");
        TYPE_MAP.put(ColumnType.VARCHAR, "VARCHAR2");
        TYPE_MAP.put(ColumnType.BLOB, "BLOB");
        TYPE_MAP.put(ColumnType.CLOB, "CLOB");
        TYPE_MAP.put(ColumnType.TIMESTAMP, "TIMESTAMP");
        TYPE_MAP.put(ColumnType.ID, "NUMBER");
        TYPE_MAP.put(ColumnType.UUID_STR, "VARCHAR2");
    }

    protected EnumMap<ColumnType, String> getTypeMappings() {
        return TYPE_MAP;
    }

    @Override
    public String tableExistenceQuery() {
        return "select 1 from user_tables where table_name = ?";
    }

    @Override
    public String indexExistenceQuery() {
        return "select 1 from user_indexes where index_name = ? and table_name = ?";
    }

    @Override
    public Map<String, ColumnDescription> getColumns(String tableName, JdbcTemplate template) {
        final Map<String, ColumnDescription> columns = new HashMap<String, ColumnDescription>();
        String sqlString = "SELECT COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,NULLABLE,DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME=?";
        template.query(sqlString, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                String columnName = rs.getString("COLUMN_NAME");
                String defaultValue = rs.getString("DATA_DEFAULT");
                if (defaultValue != null && defaultValue.trim().equals("null")) {
                    defaultValue = null;
                }
                String nativeType = rs.getString("DATA_TYPE");
                int size = ("NUMBER".equals(nativeType) ? rs.getInt("DATA_PRECISION") : rs.getInt("DATA_LENGTH"));
                columns.put(columnName, new ColumnDescription(columnName).ofSize(size).
                    setNullable("Y".equals(rs.getString("NULLABLE"))).setNativeType(nativeType).
                    ofDefaultValue(defaultValue));
            }
        }, tableName);
        return columns;
    }

    @Override
    public String getPrimaryKey(String tableName, JdbcTemplate template) {
        String sqlQuery = "SELECT CONSTRAINT_NAME FROM user_constraints WHERE TABLE_NAME = ? AND constraint_type = 'P'";
        List<String> res = template.queryForList(sqlQuery, String.class, tableName);
        if (res.size() == 0) {
            return null;
        }
        return res.get(0);
    }

    public Set<String> getIndexes(String tableName, JdbcTemplate template) {
        String sqlString = "SELECT INDEX_NAME FROM USER_IND_COLUMNS WHERE TABLE_NAME=?";
        Set<String> indexesNames = new HashSet<String>();
        List<String> indexes = template.queryForList(sqlString, String.class, tableName);
        String pk = getPrimaryKey(tableName, template);
        for (String indexName : indexes) {
            if (pk != null && pk.equalsIgnoreCase(indexName)) continue;
            indexesNames.add(indexName);
        }
        return indexesNames;
    }

    public String getAddColumnStatement(String tableName, String columnSQL) {
        return "alter table " + tableName + " add (" + columnSQL + ")";
    }

    @Override
    public boolean pkRequiresIndex() {
        return true;
    }

    @Override
    public String lowerIndex(String column) {
        return "lower(" + column +")";
    }

}
