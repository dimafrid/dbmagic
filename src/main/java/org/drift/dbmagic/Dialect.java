package org.drift.dbmagic;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

/**
 * @author Dima Frid
 */
public abstract class Dialect {
    private Logger log;

    private static final int DEFAULT_STRING_COLUMN_SIZE = 50;
    private static final int MAX_STRING_COLUMN_SIZE = 4000;
    private static final int DEFAULT_NUMBER_COLUMN_SIZE = 11;
    private static final int DEFAULT_LONG_COLUMN_SIZE = 19;
    private static final int DEFAULT_RAW_COLUMN_SIZE = 16;

    public String toNativeType(ColumnType dbtype) {
        return getTypeMappings().get(dbtype);
    }

    abstract protected EnumMap<ColumnType, String> getTypeMappings();

    public abstract String tableExistenceQuery();

    public abstract String indexExistenceQuery();

    public abstract Map<String, ColumnDescription> getColumns(String tableName, JdbcTemplate template);

    public abstract String getPrimaryKey(String tableName, JdbcTemplate template);

    public abstract Set<String> getIndexes(String tableName, JdbcTemplate template);

    public boolean tableExists(String tableName, JdbcTemplate template) {
        String sql = tableExistenceQuery();
        List<Object> res = template.queryForList(sql, Object.class, tableName);
        return res != null && !res.isEmpty();
    }

    public String toMetadataType(ColumnType type) {
        return toNativeType(type);
    }

    public String getAddColumnStatement(String tableName, String columnSQL) {
        return "";
    }

    public int getVarcharColumnSize(String tableName, String columnName, Integer requiredSize) {
        int actualSize;
        if (requiredSize == null || requiredSize == 0) {
            debug("Column [" + tableName + "." + columnName + "] is of size 0; using " + DEFAULT_STRING_COLUMN_SIZE);
            actualSize = DEFAULT_STRING_COLUMN_SIZE;
        } else if (requiredSize > MAX_STRING_COLUMN_SIZE) {
            debug("Column [" + tableName + "." + columnName + "] is of size greater than " + MAX_STRING_COLUMN_SIZE +
                     "; trimming to " + MAX_STRING_COLUMN_SIZE);
            actualSize = MAX_STRING_COLUMN_SIZE;
        } else {
            actualSize = requiredSize;
        }
        return actualSize;
    }

    public int getBooleanColumnSize() {
        return 1;
    }

    public int getVarbinaryColumnSize(Integer requiredSize) {
        if (requiredSize == null || requiredSize == 0) {
            return DEFAULT_RAW_COLUMN_SIZE;
        } else {
            return requiredSize;
        }
    }

    public int getIntegerColumnSize(Integer requiredSize) {
        if (requiredSize == null || requiredSize == 0) {
            return DEFAULT_NUMBER_COLUMN_SIZE;
        } else {
            return getNumOfDigits(requiredSize);
        }
    }

    public int getLongColumnSize(Integer requiredSize) {
        if (requiredSize == null || requiredSize == 0) {
            return DEFAULT_LONG_COLUMN_SIZE;
        } else {
            return getNumOfDigits(requiredSize);
        }
    }

    private static int getNumOfDigits(Integer size) {
        int numOfDigits = (int) (Math.log(size.doubleValue()) / Math.log((double) 10));
        return numOfDigits + 2;
    }

    public abstract boolean pkRequiresIndex();

    public boolean indexExists(String indexName, String tableName, JdbcTemplate template) {
        String sql = indexExistenceQuery();
        List<Object> res = template.queryForList(sql, Object.class, indexName, tableName);
        return res != null && !res.isEmpty();
    }

    public String lowerIndex(String column) {
        return "(" + column + ")"; // default is not supported -> return column name
    }

    public void addCheckConstraints(TableDescription tableDescription, StringBuffer sql, String expressionSeparator) {
    }

    public void setLog(Logger log) {
        this.log = log;
    }

    protected void debug(String msg) {
        if (log != null) {
            log.debug(msg);
        }
    }
}
