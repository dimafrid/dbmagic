package org.drift.dbmagic;

import java.io.Serializable;

/**
 * @author Dima Frid
 */
public enum ColumnType implements Serializable {

    BOOLEAN,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    VARCHAR,
    VARBINARY,
    CLOB,
    BLOB,
    TIMESTAMP,
    UUID_STR,
    ID
}
