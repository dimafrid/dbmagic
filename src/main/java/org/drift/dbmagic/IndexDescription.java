package org.drift.dbmagic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Dima Frid
 */
public class IndexDescription implements Serializable {

    private List<String> columnNames = new ArrayList<String>();
    private boolean unique;
    private boolean lower;

    public IndexDescription(String... columnNames) {
        this.columnNames.addAll(Arrays.asList(columnNames));
    }

    public IndexDescription addColumn(String columnName) {
        columnNames.add(columnName);
        return this;
    }

    public IndexDescription unique() {
        unique = true;
        return this;
    }

    public boolean isUnique() {
        return unique;
    }

    public IndexDescription lower() {
        lower = true;
        return this;
    }

    public boolean isLower() {
        return lower;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexDescription that = (IndexDescription) o;

        if (unique != that.unique) return false;
        if (lower != that.lower) return false;
        if (columnNames != null ? !columnNames.equals(that.columnNames) : that.columnNames != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = columnNames != null ? columnNames.hashCode() : 0;
        result = 31 * result + (unique ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return columnNames.toString() + (isUnique() ? "; unique" : "") +  (isLower() ? "; lower" : "");
    }
}
