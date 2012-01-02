package org.drift.dbmagic;

import org.drift.dbmagic.utils.StringUtils;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;

/**
 * @author Dima Frid
 */
public class TableDescription implements Serializable {

    private String tableName;
    private Map<String, ColumnDescription> columns = new LinkedHashMap<String, ColumnDescription>();
    private IndexDescription primaryKey = null;
    private Collection<IndexDescription> indexes = new ArrayList<IndexDescription>();
    private Collection<String> checks = new ArrayList<String>();
    private Collection<TableDescription> joinTablesDescriptions = new ArrayList<TableDescription>();
    private String schema;
    private String parentTableName;

    public TableDescription(String tableName) {
        this(tableName, null);
    }

    public TableDescription(String tableName, String schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullTableName() {
        return getSchemaPrefix() + tableName;
    }

    public String getFullIndexName(String indexName) {
        return getSchemaPrefix() + indexName;
    }

    private String getSchemaPrefix() {
        return (StringUtils.isEmpty(schema) ? "" : schema + ".");
    }

    public ColumnDescription addColumn(String columnName) {
        ColumnDescription columnDescription = new ColumnDescription(columnName);
        addColumn(columnDescription);
        return columnDescription;
    }

    public void addColumn(ColumnDescription columnDescription) {
        columns.put(columnDescription.getName(), columnDescription);
    }

    public void setColumns(Map<String, ColumnDescription> columns) {
        this.columns = columns;
    }

    public Map<String, ColumnDescription> getColumns() {
        return columns;
    }

    public void setPrimaryKey(IndexDescription primaryKey) {
        if (primaryKey != null) {
            primaryKey.unique();
        }
        this.primaryKey = primaryKey;
    }

    public IndexDescription getPrimaryKey() {
        return primaryKey;
    }

    public void addIndex(IndexDescription indexDescription) {
        indexes.add(indexDescription);
    }

    public IndexDescription addIndex(String columnName) {
        IndexDescription indexDescription = new IndexDescription(columnName);
        addIndex(indexDescription);
        return indexDescription;
    }

    public void setIndexes(Collection<IndexDescription> indexes) {
        this.indexes = indexes;
    }

    public Collection<IndexDescription> getIndexes() {
        return indexes;
    }

    @Override
    public String toString() {
        return "TableDescription{" +
               "tableName='" + tableName + '\'' +
               '}';
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Collection<TableDescription> getJoinTablesDescriptions() {
        return joinTablesDescriptions;
    }

    public void setJoinTablesDescriptions(Collection<TableDescription> joinTablesDescriptions) {
        this.joinTablesDescriptions = joinTablesDescriptions;
    }

    public void addJoinTableDescription(TableDescription joinTableDescription) {
        joinTablesDescriptions.add(joinTableDescription);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableDescription that = (TableDescription) o;

        if (!columns.equals(that.columns)) return false;
        if (indexes != null ? !indexes.equals(that.indexes) : that.indexes != null) return false;
        if (joinTablesDescriptions != null ? !joinTablesDescriptions.equals(that.joinTablesDescriptions) : that.joinTablesDescriptions != null)
            return false;
        if (!primaryKey.equals(that.primaryKey)) return false;
        if (!tableName.equals(that.tableName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + columns.hashCode();
        result = 31 * result + primaryKey.hashCode();
        result = 31 * result + (indexes != null ? indexes.hashCode() : 0);
        result = 31 * result + (joinTablesDescriptions != null ? joinTablesDescriptions.hashCode() : 0);
        return result;
    }

    public void print(PrintStream out) {
        ArrayList<String> list = new ArrayList<String>(getColumns().keySet());
        out.println("Table " + getTableName() + " has " + list.size() + " columns:");
        Collections.sort(list);
        for (String s : list) {
            out.println(s);
        }
    }

    public void inheritsFrom(String parentTableName) {
        this.parentTableName = parentTableName;
        setColumns(Collections.<String, ColumnDescription>emptyMap());
    }

    public String getParentTableName() {
        return parentTableName;
    }

    public boolean inherits() {
        return StringUtils.isNotEmpty(parentTableName);
    }

    public Collection<String> getChecks() {
        return checks;
    }

    public void addCheck(String check) {
        checks.add(check);
    }

    public void setChecks(Collection<String> checks) {
        this.checks = checks;
    }
}
