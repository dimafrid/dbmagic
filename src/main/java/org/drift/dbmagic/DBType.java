package org.drift.dbmagic;


/**
 * @author Dima Frid
 */
public enum DBType {

    ORACLE("Oracle"),
    POSTGRESQL("PostgreSQL"),
    H2("H2");

    private final String displayName;

    private DBType(String displayName) {
        this.displayName = displayName;
    }

    public boolean isOracle() {
        return this == ORACLE;
    }

    public boolean isPostgre() {
        return this == POSTGRESQL;
    }

    public boolean isH2() {
        return this == H2;
    }

    public static DBType getDbTypeByName(String string) {
        for (DBType dbType : DBType.values()) {
            if (dbType.displayName.equalsIgnoreCase(string)) {
                return dbType;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
