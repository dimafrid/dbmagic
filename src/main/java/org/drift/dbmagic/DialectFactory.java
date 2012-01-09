package org.drift.dbmagic;

import java.util.EnumMap;

/**
 * @author Dima Frid
 */
public class DialectFactory {
    private final static EnumMap<DBType, Dialect> map = new EnumMap<DBType, Dialect>(DBType.class);

    static {
        map.put(DBType.ORACLE, new OracleDialect());
        map.put(DBType.POSTGRESQL, new PostgreSQLDialect());
        map.put(DBType.H2, new H2Dialect());
    }

    public static Dialect getDialect(String dialectName) {
        return map.get(DBType.getDbTypeByName(dialectName));
    }
}
