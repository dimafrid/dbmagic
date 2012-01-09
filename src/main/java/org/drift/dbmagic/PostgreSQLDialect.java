package org.drift.dbmagic;

import org.drift.dbmagic.utils.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Dima Frid
 */
public class PostgreSQLDialect extends Dialect {

    private static final EnumMap<ColumnType, String> TYPE_MAP = new EnumMap<ColumnType, String>(ColumnType.class);

    private static final Map<String, String> CATALOG_TYPES = new HashMap<String, String>();

    private static final String VARCHAR_METADATA_NOTATION = "character varying";

    static {
        TYPE_MAP.put(ColumnType.BOOLEAN, "BOOLEAN");
        TYPE_MAP.put(ColumnType.INTEGER, "INTEGER");
        TYPE_MAP.put(ColumnType.LONG, "BIGINT");
        TYPE_MAP.put(ColumnType.FLOAT, "DOUBLE");
        TYPE_MAP.put(ColumnType.DOUBLE, "DOUBLE PRECISION");
        TYPE_MAP.put(ColumnType.DATE, "TIMESTAMP WITHOUT TIME ZONE");
        TYPE_MAP.put(ColumnType.VARBINARY, "BYTEA");
        TYPE_MAP.put(ColumnType.VARCHAR, "VARCHAR");
        TYPE_MAP.put(ColumnType.BLOB, "BYTEA");
        TYPE_MAP.put(ColumnType.CLOB, "TEXT");
        TYPE_MAP.put(ColumnType.TIMESTAMP, "TIMESTAMP WITH TIME ZONE");
        TYPE_MAP.put(ColumnType.ID, "BIGINT");
        TYPE_MAP.put(ColumnType.UUID_STR, "VARCHAR");
    }

    static {
        CATALOG_TYPES.put("VARCHAR", VARCHAR_METADATA_NOTATION);
    }

    protected EnumMap<ColumnType, String> getTypeMappings() {
        return TYPE_MAP;
    }

    @Override
    public String tableExistenceQuery() {
        return "select 1 from pg_tables where tablename = ?";
    }

    @Override
    public String indexExistenceQuery() {
        return "select 1 from pg_indexes where indexname = ? and tablename = ?";
    }

    @Override
    public Map<String, ColumnDescription> getColumns(String tableName, JdbcTemplate template) {
        return getColumnsFromCatalog(tableName, template);
    }

    private Map<String, ColumnDescription> getColumnsFromCatalog(String tableName, JdbcTemplate template) {
        final Map<String, ColumnDescription> columns = new HashMap<String, ColumnDescription>();
        String sqlString = "select * from information_schema.columns where table_name = ?";
        template.query(sqlString, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                String columnName = rs.getString("column_name").toUpperCase();
                String defaultValue = removeTypeDescriptionFromValue(rs.getString("column_default"));

                String nativeType = rs.getString("data_type");
                int size = (VARCHAR_METADATA_NOTATION.equals(nativeType) ? rs.getInt("character_maximum_length") : 0);

                String isNullable = rs.getString("is_nullable");
                columns.put(columnName, new ColumnDescription(columnName).setNullable(booleanValue(isNullable))
                            .ofSize(size).setNativeType(nativeType).ofDefaultValue(defaultValue));
            }
        }, tableName.toLowerCase());
        return columns;
    }

    private boolean booleanValue(String value) {
        return "true".equalsIgnoreCase(value) || "t".equalsIgnoreCase(value) || "1".equalsIgnoreCase(value)
            || "yes".equalsIgnoreCase(value);
    }

    private String removeTypeDescriptionFromValue(String value) {
        if (!StringUtils.isEmpty(value)) {
            int pos = value.indexOf("::");
            if (pos != -1) {
                value = value.substring(0, pos);
            }
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    @Override
    public String getPrimaryKey(String tableName, JdbcTemplate template) {
        String sqlQuery = "select conname from pg_constraint where conrelid = (select oid from pg_class where relname = ?) and contype = 'p'";
        List<String> res = template.queryForList(sqlQuery, String.class, tableName.toLowerCase());
        if (res.size() == 0) {
            return null;
        }
        return res.get(0);
    }

    public Set<String> getIndexes(String tableName, JdbcTemplate template) {
        String sqlString = "select indexname from pg_indexes where tablename = ?";
        Set<String> indexesNames = new HashSet<String>();
        List<String> indexes = template.queryForList(sqlString, String.class, tableName.toLowerCase());
        String pk = getPrimaryKey(tableName, template);
        for (String indexName : indexes) {
            if (pk != null && pk.equalsIgnoreCase(indexName)) continue;
            indexesNames.add(indexName);
        }
        return indexesNames;
    }

    @Override
    public boolean tableExists(String tableName, JdbcTemplate template) {
        return super.tableExists(tableName.toLowerCase(), template);
    }

    @Override
    public String toMetadataType(ColumnType type) {
        String nativeType = getTypeMappings().get(type);
        String metadataType = CATALOG_TYPES.get(nativeType);
        if (metadataType == null) {
            return nativeType;
        }
        return metadataType;
    }

    public String getAddColumnStatement(String tableName, String columnSQL) {
        return "alter table " + tableName + " add column " + columnSQL;
    }

    @Override
    public int getVarbinaryColumnSize(Integer requiredSize) {
        return 0;
    }

    @Override
    public int getIntegerColumnSize(Integer requiredSize) {
        return 0;
    }

    @Override
    public int getLongColumnSize(Integer requiredSize) {
        return 0;
    }

    @Override
    public int getBooleanColumnSize() {
        return 0;
    }

    @Override
    public boolean pkRequiresIndex() {
        return false;
    }

    @Override
    public boolean indexExists(String indexName, String tableName, JdbcTemplate template) {
        String sql = indexExistenceQuery();
        List<Object> res = template.queryForList(sql, Object.class, indexName.toLowerCase(), tableName.toLowerCase());
        return res != null && !res.isEmpty();
    }

    @Override
    public String lowerIndex(String column) {
        return "((lower(" + column + ")))";
    }

    @Override
    public void addCheckConstraints(TableDescription tableDescription, StringBuffer sql, String expressionSeparator) {
        for (String check : tableDescription.getChecks()) {
            sql.append(expressionSeparator);
            sql.append("check (" + check + ")");
            expressionSeparator = ", ";
        }
    }

}