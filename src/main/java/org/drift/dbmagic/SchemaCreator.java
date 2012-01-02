package org.drift.dbmagic;

import org.apache.log4j.Logger;
import org.drift.dbmagic.utils.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Dima Frid
 */
public class SchemaCreator {
    private static final int UUID_STR_COLUMN_SIZE = 37;

    private Logger log;

    private DataSource dataSource;

    private JdbcTemplate template;

    private String dialectName;

    public List<TableDescription> createTablesFromJPA(Collection<Class<?>> classes) {
        Set<TableDescription> tables = new HashSet<TableDescription>();
        for (Class<?> clazz : classes) {
            int modifiers = clazz.getModifiers();
            if (DBTable.class.isAssignableFrom(clazz) && !Modifier.isAbstract(modifiers)) {
                try {
                    Method method = clazz.getMethod("tableDescription");
                    TableDescription tableDescription = (TableDescription) method.invoke(clazz.newInstance());
                    tables.add(tableDescription);
                } catch (Exception e) {
                    log.error("Failed to retrieve table description for class: " + clazz, e);
                }
            }
        }

        return createTables(tables);
    }

    public List<TableDescription> createTables(Collection<TableDescription> tables) {
        List<TableDescription> successful = new ArrayList<TableDescription>();
        for (TableDescription table : tables) {
            try {
                _createTable(table);
                successful.add(table);
            } catch (Exception e) {
                log.error("Failed to create or update table: " + table, e);
            }
        }

        return successful;
    }

    public void createTable(DBTable dbTable) throws SQLException {
        TableDescription tableDescription = dbTable.tableDescription();
        _createTable(tableDescription);
    }

    public void createTable(TableDescription tableDescription) throws SQLException {
        _createTable(tableDescription);
    }

    private void _createTable(TableDescription tableDescription) throws SQLException {
        String tableName = tableDescription.getTableName();

        if (!tableExists(tableName)) {
            String sql = composeTableCreationSql(tableDescription);
            log.info("Creating table " + tableName);
            template.execute(sql);

            for (ColumnDescription column : tableDescription.getColumns().values()) {
                setCompressedStorage(tableDescription, column);
            }

            createPrimaryKey(tableDescription);
            createIndexes(tableDescription);
        } else {
            updateTable(tableDescription);
        }

        Collection<TableDescription> joinTablesDescriptions = tableDescription.getJoinTablesDescriptions();
        if (joinTablesDescriptions == null) {
            return;
        }
        for (TableDescription joinTableDescription : joinTablesDescriptions) {
            _createTable(joinTableDescription);
        }
    }

    private void updateTable(TableDescription tableDescription) throws SQLException {
        if (DBType.getDbTypeByName(getDialectName()).isH2()) {
            return;
        }

        String tableName = tableDescription.getTableName();
        Map<String, ColumnDescription> existingColumns = getDialect().getColumns(tableName, template);
        for (ColumnDescription columnDescription : tableDescription.getColumns().values()) {
            ColumnDescription existingColumn = existingColumns.get(columnDescription.getName());
            if (existingColumn != null) {
                updateColumn(tableName, existingColumn, columnDescription);
                continue;
            }

            createColumn(tableDescription, columnDescription);
        }

        Set<String> existingIndexes = getDialect().getIndexes(tableName, template);
        boolean newIndexFound = false;
        for (IndexDescription indexDescription : tableDescription.getIndexes()) {
            String requiredIndexName = SchemaUtil.composeIndexName(indexDescription, tableName);
            if (!existingIndexes.contains(requiredIndexName) && !existingIndexes.contains(requiredIndexName.toLowerCase())) {
                newIndexFound = true;
                break;
            }
        }

        if (newIndexFound) {
            dropIndexes(existingIndexes, tableDescription);
            createIndexes(tableDescription);
        }

        String existingPK = getDialect().getPrimaryKey(tableName, template);
        String pk = SchemaUtil.composePKName(tableName);
        if (existingPK != null && !pk.equalsIgnoreCase(existingPK)) {
            dropPK(existingPK, tableName);
            createPrimaryKey(tableDescription);
        }
    }

    private void dropPK(String pkName, String tableName) {
        log.info("Dropping constraint " + pkName);
        String sql = "alter table " + tableName + " drop constraint " + pkName;
        template.execute(sql);
    }

    private boolean sameBooleanValues(String required, String fromDB) {
        Boolean requiredAsBool = "true".equalsIgnoreCase(required) || "t".equalsIgnoreCase(required)
                                 || "1".equalsIgnoreCase(required);
        Boolean fromDBAsBool = "true".equalsIgnoreCase(fromDB) || "t".equalsIgnoreCase(fromDB)
                               || "1".equalsIgnoreCase(fromDB);
        return requiredAsBool.equals(fromDBAsBool);
    }

    private void updateColumn(String tableName, ColumnDescription existingColumnDescription, ColumnDescription columnDescription) {
        String required = getDialect().toMetadataType(columnDescription.getType());
        String actual = existingColumnDescription.getNativeType();
        if (!required.equalsIgnoreCase(actual)) {
            log.error("Column type change [" + actual + " -> " + required + "] is not supported within the scope of schema upgrade");
            return;
        }

        int requiredSize = getColumnSize(columnDescription, tableName);
        Integer existingSize = existingColumnDescription.getSize();

        boolean useSize;
        if (SchemaUtil.isLOB(columnDescription)) {
            useSize = false;
        } else {
            if (requiredSize > 0 && requiredSize < existingSize) {
                log.info("size (" + existingSize + "->" + requiredSize + ") decrease is not supported; column [" + columnDescription + "]");
            }
            useSize = requiredSize > existingSize;
        }

        if (existingColumnDescription.isNullable() && !columnDescription.isNullable()) {
            log.info("The constraint change (NULLABLE -> NOT NULLABLE) is unsupported; column [" + columnDescription + "]");
        }
        boolean useConstraint = (!existingColumnDescription.isNullable() && columnDescription.isNullable());

        String defaultValue = columnDescription.getDefaultValue();
        String existingDefaultValue = existingColumnDescription.getDefaultValue();
        boolean useDefaultValue = false;
        if (defaultValue != null) {
            if (!defaultValue.equals(existingDefaultValue)) {
                useDefaultValue = true;
            }
        } else {
            useDefaultValue = (existingDefaultValue != null);
        }

        if (isPostgre()) {
            String prefix = "alter table " + tableName + " alter column " + columnDescription.getName();
            if (useSize) {
                StringBuilder sql = new StringBuilder(prefix + " type ");
                sql.append(" " + getDialect().toNativeType(columnDescription.getType()));
                int columnSize = getColumnSize(columnDescription, tableName);
                if (columnSize > 0) {
                    sql.append("(" + columnSize + ")");
                }
                template.execute(sql.toString());
                log.info("Column [" + columnDescription.getName() + "] size updated to " + columnDescription.getSize());
            }

            if (useConstraint) {
                String sql = prefix + " drop not null";
                template.execute(sql);
                log.info("Column [" + columnDescription.getName() + "] nullable constraint dropped");
            }

            if (columnDescription.getType().equals(ColumnType.BOOLEAN)) {
                useDefaultValue = !sameBooleanValues(defaultValue, existingDefaultValue);
            }

            if (useDefaultValue) {
                if (defaultValue == null) {
                    String sql = prefix + " drop default";
                    template.execute(sql);
                    log.info("Column [" + columnDescription.getName() + "] default value dropped");
                } else {
                    String sql = prefix + " set default '" + columnDescription.getDefaultValue() + "'";
                    template.execute(sql);
                    log.info("Column [" + columnDescription.getName() + "] default value updated to " + columnDescription.getDefaultValue());
                }
            }
        } else if (isOracle()) {
            if (useSize || useConstraint || useDefaultValue) {
                String sql = "alter table " + tableName + " modify (" +
                             composeColumnSQL(tableName, columnDescription, useSize, useConstraint, useDefaultValue) + ")";
                template.execute(sql);
                log.info("Column updated: " + columnDescription);
            }
        }
    }

    private void dropIndexes(Set<String> existingIndexes, TableDescription tableDescription) {
        for (String existingIndex : existingIndexes) {
            dropIndex(existingIndex, tableDescription);
        }
    }

    private void dropIndex(String index, TableDescription tableDescription) {
        log.info("Dropping index " + index);
        if (indexExists(index, tableDescription.getTableName())) {
            template.execute("drop index " + tableDescription.getFullIndexName(index));
        }
    }

    private void createColumn(TableDescription table, ColumnDescription columnDescription) {
        String tableName = table.getFullTableName();
        String columnSQL = composeColumnSQL(tableName, columnDescription, true);
        String sql = getDialect().getAddColumnStatement(tableName, columnSQL);
        log.info("Creating column [" + columnDescription + "] in table [" + tableName + "]");
        template.execute(sql);

        setCompressedStorage(table, columnDescription);
    }

    private void setCompressedStorage(TableDescription table, ColumnDescription columnDescription) {
        if (columnDescription.isCompressed() && isPostgre()) {
            String sql = "alter table " + table.getFullTableName() + " alter column " + columnDescription.getName() + " set storage external";
            template.execute(sql);
        }
    }

    private String composeTableCreationSql(TableDescription tableDescription) {
        StringBuffer sql = new StringBuffer("create table ");

        String tableName = tableDescription.getFullTableName();
        sql.append(tableName);
        sql.append(" (");

        String comma = "";
        for (ColumnDescription columnDescription : tableDescription.getColumns().values()) {
            sql.append(comma);
            sql.append(composeColumnSQL(tableName, columnDescription));
            comma = ", ";
        }

        getDialect().addCheckConstraints(tableDescription, sql, comma);

        sql.append(")");

        if (isPostgre() && tableDescription.inherits()) {
            sql.append(" inherits (" + tableDescription.getParentTableName() + ")");
        }

        sql.append(getDialect().engine());

        return sql.toString();
    }

    private String composeColumnSQL(String tableName, ColumnDescription columnDescription) {
        boolean useDefaultValue = !StringUtils.isEmpty(columnDescription.getDefaultValue());
        return composeColumnSQL(tableName, columnDescription, true, true, useDefaultValue);
    }

    private String composeColumnSQL(String tableName, ColumnDescription columnDescription, boolean useConstraints) {
        boolean useDefaultValue = !StringUtils.isEmpty(columnDescription.getDefaultValue());
        return composeColumnSQL(tableName, columnDescription, true, useConstraints || useDefaultValue, useDefaultValue);
    }

    private String composeColumnSQL(String tableName, ColumnDescription columnDescription, boolean useSize, boolean useConstraint, boolean useDefaultValue) {
        StringBuilder sql = new StringBuilder(columnDescription.getName());

        if (useSize) {
            sql.append(" " + getDialect().toNativeType(columnDescription.getType()));
            int columnSize = getColumnSize(columnDescription, tableName);
            if (columnSize > 0) {
                sql.append("(" + columnSize + ")");
            }
        }

        if (useDefaultValue) {
            String defaultValue = columnDescription.getDefaultValue();
            if (columnDescription.getType() == ColumnType.LONG || columnDescription.getType() == ColumnType.INTEGER) {
                sql.append(" DEFAULT " + (defaultValue == null ? "null" : defaultValue));
            } else {
                sql.append(" DEFAULT " + (defaultValue == null ? "null" : "'" + defaultValue + "'"));
            }
        }

        if (useConstraint) {
            if (columnDescription.isNullable()) {
                sql.append(" null");
            } else {
                sql.append(" not null");
            }
        }
        return sql.toString();
    }

    private int getColumnSize(ColumnDescription columnDescription, String tableName) {
        int columnSize = 0;

        String columnName = columnDescription.getName();
        ColumnType type = columnDescription.getType();
        Integer size = columnDescription.getSize();

        switch (type) {
            case BOOLEAN:
                columnSize = getDialect().getBooleanColumnSize();
                break;
            case VARBINARY:
                columnSize = getDialect().getVarbinaryColumnSize(size);
                break;
            case INTEGER:
                columnSize = getDialect().getIntegerColumnSize(size);
                break;
            case LONG:
                columnSize = getDialect().getLongColumnSize(size);
                break;
            case ID:
                columnSize = getDialect().getLongColumnSize(size);
                break;
            case VARCHAR:
                columnSize = getDialect().getVarcharColumnSize(tableName, columnName, size);
                break;
            case UUID_STR:
                columnSize = UUID_STR_COLUMN_SIZE;
                break;
        }

        return columnSize;
    }

    private boolean isPostgre() {
        return DBType.getDbTypeByName(getDialectName()).isPostgre();
    }

    private boolean isOracle() {
        return DBType.getDbTypeByName(getDialectName()).isOracle();
    }

    public boolean tableExists(String tableName) {
        return getDialect().tableExists(tableName, template);
    }

    public boolean indexExists(String indexName, String tableName) {
        return getDialect().indexExists(indexName, tableName, template);
    }

    private Dialect getDialect() {
        return DialectFactory.getDialect(dialectName);
    }

    public void createPrimaryKey(TableDescription tableDescription) {
        IndexDescription pk = tableDescription.getPrimaryKey();
        if (pk == null) {
            return;
        }

        String fullTableName = tableDescription.getFullTableName();
        String pkName = SchemaUtil.composePKName(tableDescription.getTableName());

        log.info("Creating primary key [" + pkName + "] for table " + fullTableName);

        if (getDialect().pkRequiresIndex()) {
            createIndex(fullTableName, pk, pkName);
        }

        addPKConstraint(fullTableName, pk, pkName);
    }

    private void addPKConstraint(String tableName, IndexDescription primaryKey, String pkName) {
        log.info("Adding PK constraint [" + pkName + "] for table " + tableName);

        StringBuilder sql = new StringBuilder();

        sql.append("alter table ").append(tableName);

        sql.append(" add constraint ");
        sql.append(pkName);
        sql.append(" primary key (");
        sql.append(appendColumnNames(primaryKey));
        sql.append(")");

        template.execute(sql.toString());
    }

    private String appendColumnNames(IndexDescription indexDescription) {
        StringBuilder sqlString = new StringBuilder();
        String comma = "";
        for (String columnName : indexDescription.getColumnNames()) {
            sqlString.append(comma);
            sqlString.append(columnName);
            comma = ", ";
        }
        return sqlString.toString();
    }

    private void createIndexes(TableDescription tableDescription) {
        for (IndexDescription indexDescription : tableDescription.getIndexes()) {
            createIndex(tableDescription, indexDescription);
        }
    }

    public void createIndex(TableDescription tableDescription, IndexDescription indexDescription) {
        String indexName = SchemaUtil.composeIndexName(indexDescription, tableDescription.getTableName());
        createIndex(tableDescription.getFullTableName(), indexDescription, indexName);
    }

    public void createIndex(String tableName, IndexDescription indexDescription, String indexName) {
        if (!indexExists(indexName, tableName)) {
            String sql = composeIndexCreationSql(indexDescription, indexName, tableName);
            log.info("Creating " + indexName + " index [" + indexDescription + "] for table " + tableName);
            template.execute(sql);
        }
    }

    private String composeIndexCreationSql(IndexDescription indexDescription, String indexName, String tableName) {
        StringBuilder sql = new StringBuilder();

        sql.append("create ");
        if (indexDescription.isUnique()) {
            sql.append("unique ");
        }
        sql.append("index ");
        sql.append(indexName);

        sql.append(" on ").append(tableName);
        if (indexDescription.isLower()) {
            if (indexDescription.getColumnNames().size() != 1) {
                log.error("Can't have lower flag on composite index. " + indexDescription);
            } else {
                String column = indexDescription.getColumnNames().iterator().next();
                String index = getDialect().lowerIndex(column);
                sql.append(" ").append(index);
            }
        } else {

            sql.append(" (");

            String sep = "";
            for (String column : indexDescription.getColumnNames()) {
                sql.append(sep);
                sql.append(column);
                sep = ", ";
            }
            sql.append(")");
        }

        return sql.toString();
    }

    public String getDialectName() {
        return dialectName;
    }
}
