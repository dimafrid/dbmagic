package org.drift.dbmagic;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @author Dima Frid
 */
@Table(name = "TstJPAEntity")
public class TstJPAEntity {
    private static enum Status {
        GOOD, BAD
    }

    private long id;

    private String stringField;

    private Timestamp timestampField;

    private boolean booleanField;

    private String lobField;

    private Status enumField;

    @Column(name = "BOOL")
    public boolean isBooleanField() {
        return booleanField;
    }

    @Column
    @Enumerated(EnumType.STRING)
    public Status getEnumField() {
        return enumField;
    }

    @Id
    public long getId() {
        return id;
    }

    @Lob
    public String getLobField() {
        return lobField;
    }

    @Column
    public String getStringField() {
        return stringField;
    }

    @Column
    public Timestamp getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(Timestamp timestampField) {
        this.timestampField = timestampField;
    }

    public void setBooleanField(boolean booleanField) {
        this.booleanField = booleanField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public void setLobField(String lobField) {
        this.lobField = lobField;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setEnumField(Status enumField) {
        this.enumField = enumField;
    }
}
