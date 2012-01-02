package org.drift.dbmagic;

/**
 * @author Dima Frid
 */
public class SchemaUtil {
    private static final String PRIMARY_KEY_PREFIX = "PK_";

    private static final String INDEX_PREFIX = "IX1_";
    private static final String UNIQUE_INDEX_PREFIX = "UIX1_";

    public static String composePKName(String tableName) {
        return PRIMARY_KEY_PREFIX + tableName;
    }

    public static String composeIndexName(IndexDescription indexDescription, String tableName) {
        String prefix = indexDescription.isUnique() ? UNIQUE_INDEX_PREFIX : INDEX_PREFIX;
        StringBuilder buf = new StringBuilder(prefix).append(tableName);
        for (String columnName : indexDescription.getColumnNames()) {
            buf.append("_").append(columnName);
        }
        if (indexDescription.isLower()) {
            buf.append("_LOWER");
        }
        return buf.toString().toUpperCase();
    }

    public static boolean isLOB(ColumnDescription column) {
        return column.getType().equals(ColumnType.BLOB) || column.getType().equals(ColumnType.CLOB);
    }
}
