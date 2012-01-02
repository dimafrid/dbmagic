package org.drift.dbmagic;

/**
 * @author Dima Frid
 */
class TstDBTable implements DBTable {
    static final String TABLE_NAME = "TST_TABLE";

    @Override
    public TableDescription tableDescription() {
        return new TableDescription(TABLE_NAME);
    }
}
