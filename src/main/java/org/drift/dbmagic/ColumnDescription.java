package org.drift.dbmagic;

import java.io.Serializable;

/**
 * @author Dima Frid
 */
public class ColumnDescription implements Serializable {

    private String name;
    private ColumnType dbType;
    private Integer size;
    private boolean isNullable = true;
    private String defaultValue = null;
    private String nativeType;
    private boolean isCompressed = false;

    public ColumnDescription(String name) {
        this.name = name.toUpperCase();
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return dbType;
    }

    public ColumnDescription ofType(ColumnType type) {
        this.dbType = type;
        setTypeConstraints();
        return this;
    }

    private void setTypeConstraints() {
        switch (getType()) {
            case BOOLEAN:
                size = 1;
                notNullable();
                if (getDefaultValue() == null) {
                    setDefaultValue("0");
                }
                break;
            case ID:
                notNullable();
                break;
            case UUID_STR:
                notNullable();
                break;
        }
    }

    public Integer getSize() {
        return size;
    }

    public ColumnDescription ofSize(int size) {
        this.size = size;
        return this;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public ColumnDescription setNullable(boolean nullable) {
        isNullable = nullable;
        return this;
    }

    public ColumnDescription nullable() {
        isNullable = true;
        return this;
    }

    public ColumnDescription notNullable() {
        isNullable = false;
        return this;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public ColumnDescription ofDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public boolean isDefaultSize() {
        return size == null;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setCompressed(boolean compressed) {
        isCompressed = compressed;
    }

    @Override
    public String toString() {
        return "name = " + name + ", dbType = " + dbType + ", nativeType = " + nativeType + ", size = " + size + ", isNullable = " + isNullable + ", defaultValue = " + defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDescription that = (ColumnDescription) o;

        if (isNullable != that.isNullable) return false;
        if (dbType != that.dbType) return false;
        if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null) return false;
        if (!name.equals(that.name)) return false;
        if (size != null ? !size.equals(that.size) : that.size != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + dbType.hashCode();
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (isNullable ? 1 : 0);
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        return result;
    }

    public String getNativeType() {
        return nativeType;
    }

    public ColumnDescription setNativeType(String nativeType) {
        this.nativeType = nativeType;
        return this;
    }
}
