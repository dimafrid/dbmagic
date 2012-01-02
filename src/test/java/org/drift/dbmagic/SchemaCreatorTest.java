package org.drift.dbmagic;

import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Dima Frid
 */
public class SchemaCreatorTest {
    private static String ID_COL_NAME = "ID";
    private static String ID1_COL_NAME = "ID1";
    private static String INT_COL_NAME = "INT_COL";
    private static String STR_COL_NAME = "STR_COL";
    private static String STR1_COL_NAME = "STR1_COL";
    private static String BOOL_COL_NAME = "BOOL_COL";
    private static String TIME_COL_NAME = "TIME";

    private SchemaCreator schemaCreator;

    @PersistenceContext
    private EntityManager entityManager;

    private TstDBTable table = new TstDBTable() {
        @Override
        public TableDescription tableDescription() {
            TableDescription tableDescription = new TableDescription(TABLE_NAME);

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

        TstDBTable table1 = new TstDBTable() {
            @Override
            public TableDescription tableDescription() {
                TableDescription tableDescription = new TableDescription(TABLE_NAME);

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

        table1 = new TstDBTable() {
            @Override
            public TableDescription tableDescription() {
                TableDescription tableDescription = new TableDescription(TABLE_NAME);

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

        // Test upgrade boolean with data
        final TableDescription table2 = new TableDescription("bool_upgr");
        table2.addColumn("id").ofType(ColumnType.ID);
        DBTable dbTable = new DBTable() { public TableDescription tableDescription() { return table2; } };
        schemaCreator.createTable(dbTable);
        entityManager.createNativeQuery("insert into bool_upgr (id) values ('1')").executeUpdate();
        table2.addColumn("boolcol1").ofType(ColumnType.BOOLEAN);
        table2.addColumn("boolcol2").ofType(ColumnType.BOOLEAN).setDefaultValue("1");
        schemaCreator.createTable(dbTable);
        Object[] result = (Object[]) entityManager.createNativeQuery("select * from bool_upgr").getSingleResult();
        assertFalse((Boolean) result[1]);        
        assertTrue((Boolean) result[2]);        
    }

    private void validateTable(TstDBTable table) {
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

//    @Test
//    public void testSchemaHash() {
//        List<TableDescription> successful = schemaCreator.createTables(table.tableDescription(), new KeyValuePair().tableDescription());
//        assertEquals(2, successful.size());
//
//        successful = schemaCreator.createTables(table.tableDescription(), new KeyValuePair().tableDescription());
//        assertEquals(0, successful.size());
//
//        successful = schemaCreator.createTables(table.tableDescription(), new KeyValuePair().tableDescription());
//        assertEquals(0, successful.size());
//    }

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
