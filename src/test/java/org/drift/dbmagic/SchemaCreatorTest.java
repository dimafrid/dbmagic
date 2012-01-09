package org.drift.dbmagic;

import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Dima Frid
 */
public class SchemaCreatorTest {
    private static final String ID_COL_NAME = "ID";
    private static final String ID1_COL_NAME = "ID1";
    private static final String INT_COL_NAME = "INT_COL";
    private static final String STR_COL_NAME = "STR_COL";
    private static final String STR1_COL_NAME = "STR1_COL";
    private static final String BOOL_COL_NAME = "BOOL_COL";

    private static final String TEST_TABLE_NAME = "TST_TABLE";

    private SchemaCreator schemaCreator;

    private DBTable table = new DBTable() {
        @Override
        public TableDescription tableDescription() {
            TableDescription tableDescription = new TableDescription(TEST_TABLE_NAME);

            tableDescription.addColumn(ID_COL_NAME).ofType(ColumnType.ID).notNullable();
            tableDescription.addColumn(INT_COL_NAME).ofType(ColumnType.INTEGER).ofSize(5);
            tableDescription.addColumn(STR_COL_NAME).ofType(ColumnType.VARCHAR).ofSize(20).notNullable();
            tableDescription.addColumn(STR1_COL_NAME).ofType(ColumnType.VARCHAR).ofDefaultValue("Chupacabra");

            tableDescription.setPrimaryKey(new IndexDescription(ID_COL_NAME));

            tableDescription.addIndex(new IndexDescription(INT_COL_NAME).addColumn(STR_COL_NAME));
            tableDescription.addIndex(new IndexDescription(STR_COL_NAME).lower());

            return tableDescription;
        }
    };

    @Before
    public void be4() {
        //setupPostgreSQL();
        setupH2();
    }

    private void setupPostgreSQL() {
        String url = "jdbc:postgresql:dbmagic";
        String user = "dima";
        String password = "dima";
        DriverManagerDataSource ds = new DriverManagerDataSource(url, user, password);
        schemaCreator = new SchemaCreator(ds, DBType.POSTGRESQL);
    }

    private void setupH2() {
        String url = "jdbc:h2:mem:test;DB_CLOSE_DELAY\\=-1;MVCC\\=TRUE";
        String user = "sa";
        String password = "";
        DriverManagerDataSource ds = new DriverManagerDataSource(url, user, password);
        schemaCreator = new SchemaCreator(ds, DBType.H2);
    }

    @Test
    public void testSimple() throws SQLException {
        schemaCreator.createTable(table);

        validateTable(table);
    }

    @Test
    public void testUpdate() throws SQLException {
        if (DBType.getDbTypeByName(schemaCreator.getDialectName()).isH2()) {
            return;
        }

        schemaCreator.createTable(table);

        DBTable table1 = new DBTable() {
            @Override
            public TableDescription tableDescription() {
                TableDescription tableDescription = new TableDescription(TEST_TABLE_NAME);

                tableDescription.addColumn(ID_COL_NAME).ofType(ColumnType.ID).notNullable();
                tableDescription.addColumn(ID1_COL_NAME).ofType(ColumnType.ID).notNullable();
                tableDescription.addColumn(INT_COL_NAME).ofType(ColumnType.INTEGER).ofSize(10);
                tableDescription.addColumn(STR_COL_NAME).ofType(ColumnType.VARCHAR).ofSize(30);
                tableDescription.addColumn(BOOL_COL_NAME).ofType(ColumnType.BOOLEAN);
                tableDescription.addColumn(STR1_COL_NAME).ofType(ColumnType.VARCHAR).ofDefaultValue("Manute");

                tableDescription.setPrimaryKey(new IndexDescription(ID1_COL_NAME));

                tableDescription.addIndex(new IndexDescription(INT_COL_NAME));
                tableDescription.addIndex(new IndexDescription(STR_COL_NAME));
                tableDescription.addIndex(new IndexDescription(STR_COL_NAME, BOOL_COL_NAME));

                return tableDescription;
            }
        };

        schemaCreator.createTable(table1);

        validateTable(table1);

        table1 = new DBTable() {
            @Override
            public TableDescription tableDescription() {
                TableDescription tableDescription = new TableDescription(TEST_TABLE_NAME);

                tableDescription.addColumn(ID_COL_NAME).ofType(ColumnType.ID).notNullable();
                tableDescription.addColumn(ID1_COL_NAME).ofType(ColumnType.ID).notNullable();
                tableDescription.addColumn(INT_COL_NAME).ofType(ColumnType.INTEGER).ofSize(10);
                tableDescription.addColumn(STR_COL_NAME).ofType(ColumnType.VARCHAR).ofSize(30);
                tableDescription.addColumn(BOOL_COL_NAME).ofType(ColumnType.BOOLEAN);
                tableDescription.addColumn(STR1_COL_NAME).ofType(ColumnType.VARCHAR).ofDefaultValue(null);

                tableDescription.setPrimaryKey(new IndexDescription(ID1_COL_NAME));

                tableDescription.addIndex(new IndexDescription(INT_COL_NAME));
                tableDescription.addIndex(new IndexDescription(STR_COL_NAME));
                tableDescription.addIndex(new IndexDescription(STR_COL_NAME, BOOL_COL_NAME));

                return tableDescription;
            }
        };

        schemaCreator.createTable(table1);

        validateTable(table1);
    }

    private void validateTable(DBTable table) {
        TableDescription tableDescription = table.tableDescription();
        String tableName = tableDescription.getTableName();

        assertTrue(schemaCreator.tableExists(tableName));

        for (IndexDescription indexDescription : tableDescription.getIndexes()) {
            String indexName = SchemaUtil.composeIndexName(indexDescription, tableName);
            assertTrue(schemaCreator.indexExists(indexName, tableName));
        }

        IndexDescription pk = tableDescription.getPrimaryKey();
        if (pk != null) {
            String pkName = SchemaUtil.composePKName(tableName);
            assertTrue(schemaCreator.indexExists(pkName, tableName));
        }
    }

    @Test
    public void testTableDescriptionCreationFromJPA() {
        TableDescription tableDescription = TableDescriptionUtil.getTableDescription(TstJPAEntity.class);
        Map<String,ColumnDescription> columns = tableDescription.getColumns();
        assertNotNull(columns);
        assertTrue(columns.size() > 0);

        ColumnDescription columnDescription = columns.get("BOOL");
        assertNotNull(columnDescription);
        assertEquals("BOOL", columnDescription.getName());
        assertEquals(ColumnType.BOOLEAN, columnDescription.getType());

        columnDescription = columns.get("ENUMFIELD");
        assertNotNull(columnDescription);
        assertEquals("ENUMFIELD", columnDescription.getName());
        assertEquals(ColumnType.VARCHAR, columnDescription.getType());

        columnDescription = columns.get("LOBFIELD");
        assertNotNull(columnDescription);
        assertEquals("LOBFIELD", columnDescription.getName());
        assertEquals(ColumnType.CLOB, columnDescription.getType());

        columnDescription = columns.get("TIMESTAMPFIELD");
        assertNotNull(columnDescription);
        assertEquals("TIMESTAMPFIELD", columnDescription.getName());
        assertEquals(ColumnType.TIMESTAMP, columnDescription.getType());

        columnDescription = columns.get("STRINGFIELD");
        assertNotNull(columnDescription);
        assertEquals("STRINGFIELD", columnDescription.getName());
        assertEquals(ColumnType.VARCHAR, columnDescription.getType());

        columnDescription = columns.get("ID");
        assertNotNull(columnDescription);
        assertEquals("ID", columnDescription.getName());
        assertEquals(ColumnType.ID, columnDescription.getType());
    }
}
