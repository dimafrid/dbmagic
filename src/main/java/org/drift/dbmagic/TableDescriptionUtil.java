package org.drift.dbmagic;

import org.drift.dbmagic.utils.ReflectionUtils;
import org.drift.dbmagic.utils.StringUtils;

import javax.persistence.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author Dima Frid
 */
public class TableDescriptionUtil {
    public static TableDescription getTableDescription(Class clazz) {

        Class theClass = clazz;

        Annotation annotation;
        while (true) {
            if (Object.class.equals(theClass)) return null;
            annotation = theClass.getAnnotation(Table.class);
            if (annotation != null) break;
            theClass = theClass.getSuperclass();
        }

        Table tableAnnotation = (Table) annotation;
        String tableName = tableAnnotation.name();
        if (StringUtils.isEmpty(tableName)) {
            return null;
        }

        TableDescription table = new TableDescription(tableName);

        if (isFieldAccess(clazz)) {
            Collection<Field> allFields = ReflectionUtils.getAllFields(clazz);
            return process(table, allFields.toArray(new Field[allFields.size()]));
        }

        if (isPropertyAccess(clazz)) {
            Method[] methods = clazz.getMethods();
            return process(table, methods);
        }

        throw new UnsupportedOperationException("Annotate something, man !!!");
    }

    private static TableDescription process(TableDescription table, AnnotatedElement[] annotatedElements) {
        for (AnnotatedElement annotatedElement : annotatedElements) {
            Lob lob = annotatedElement.getAnnotation(Lob.class);
            if (lob != null) {
                String name = getColumnNameBySpecialAnnotation(annotatedElement);
                Class<?> type = getElementType(annotatedElement);
                ColumnDescription column = table.addColumn(name);
                if (type.equals(String.class)) {
                    column.ofType(ColumnType.CLOB);
                    Compressed compressedAnnotation = annotatedElement.getAnnotation(Compressed.class);
                    if (compressedAnnotation != null) {
                        column.setCompressed(true);
                    }
                } else {
                    column.ofType(ColumnType.BLOB);
                }
                continue;
            }

            Id id = annotatedElement.getAnnotation(Id.class);
            if (id != null) {
                String name = getColumnNameBySpecialAnnotation(annotatedElement);
                ColumnType columnType = convertToColumnType(annotatedElement);
                if (columnType.equals(ColumnType.VARCHAR)) {
                    table.addColumn(name).ofType(ColumnType.UUID_STR);
                } else {
                    table.addColumn(name).ofType(ColumnType.ID);
                }
                table.setPrimaryKey(new IndexDescription(name));
                continue;
            }

            if (!annotatedElement.isAnnotationPresent(Column.class) || annotatedElement.isAnnotationPresent(ElementCollection.class))
                continue;
            Column annotation = annotatedElement.getAnnotation(Column.class);

            ColumnDescription descr = new ColumnDescription(elementName(annotatedElement));

            descr.ofSize(elementSize(annotatedElement));
            descr.setNullable(elementNullable(annotatedElement));
            descr.ofType(convertToColumnType(annotatedElement));

            table.addColumn(descr);

            if (annotation.unique()) {
                table.addIndex(new IndexDescription(descr.getName()).unique());
            }
        }
        processJoinTables(table, annotatedElements);
        return table;
    }

    private static void processJoinTables(TableDescription table, AnnotatedElement[] annotatedElements) {
        for (AnnotatedElement annotatedElement : annotatedElements) {
            JoinTable joinTable = annotatedElement.getAnnotation(JoinTable.class);
            if (joinTable != null) {
                String joinTableName = joinTable.name();
                if (joinTableName == null || joinTableName.isEmpty()) {
                    throw new IllegalArgumentException("@JoinTable annotation must include explicit table name. found on " + joinTable);
                }
                TableDescription joinTableDescription = new TableDescription(joinTableName);
                JoinColumn[] joinColumns = joinTable.joinColumns();
                JoinColumn[] inverseJoinColumns = joinTable.inverseJoinColumns();

                if (joinColumns.length == 0 && inverseJoinColumns.length == 0) {
                    return;
                }

                Set<String> columnNames = new HashSet<String>();
                for (JoinColumn joinColumn : joinColumns) {
                    columnNames.add(joinColumn.name());
                }

                for (JoinColumn inversejoinColumn : inverseJoinColumns) {
                    columnNames.add(inversejoinColumn.name());
                }

                for (String columnName : columnNames) {
                    joinTableDescription.addColumn(columnName).ofType(ColumnType.LONG).setNullable(false);
                }


                joinTableDescription.setPrimaryKey(new IndexDescription(columnNames.toArray(new String[columnNames.size()])));
                //table.addJoinTableDescription(joinTableDescription);
            }
        }
    }

    private static boolean elementNullable(AnnotatedElement annotatedElement) {
        Column annotation = annotatedElement.getAnnotation(Column.class);
        if (annotation != null) return annotation.nullable();
        throw new IllegalArgumentException("not annotated element: " + annotatedElement);
    }

    private static int elementSize(AnnotatedElement annotatedElement) {
        Column annotation = annotatedElement.getAnnotation(Column.class);
        if (annotation == null) {
            return 0;
        }

        Class<?> type = getElementType(annotatedElement);
        if (ReflectionUtils.isNumeric(type)) {
            return annotation.precision();
        }

        if (String.class.equals(type)) {
            return annotation.length();
        }

        return 0;
    }

    private static String elementName(AnnotatedElement annotatedElement) {
        Column ann = annotatedElement.getAnnotation(Column.class);
        String name;
        if (ann == null) {
            throw new IllegalArgumentException("no annotation present on: " + annotatedElement);
        }
        name = ann.name();
        if (StringUtils.isEmpty(name)) {
            name = getPropertyName(annotatedElement);
        }
        return name;
    }

    private static String getColumnNameBySpecialAnnotation(AnnotatedElement annotatedElement) {
        String name;

        Column ann = annotatedElement.getAnnotation(Column.class);
        if (ann == null) {
            name = getPropertyName(annotatedElement);
        } else {
            name = ann.name();
            if (StringUtils.isEmpty(name)) {
                name = getPropertyName(annotatedElement);
            }
        }
        return name;
    }

    private static String getPropertyName(AnnotatedElement annotatedElement) {
        if (annotatedElement instanceof Field) {
            return ((Field) annotatedElement).getName();
        } else {
            return getPropertyNameFromMethod(annotatedElement);
        }
    }

    private static String getPropertyNameFromMethod(AnnotatedElement annotatedElement) {
        String methodName = ((Method) annotatedElement).getName();
        if (methodName.startsWith("get")) {
            return methodName.substring("get".length()).toLowerCase();
        } else {
            return methodName.substring("is".length()).toLowerCase();
        }
    }

    private static ColumnType convertToColumnType(AnnotatedElement annotatedElement) {
        Class<?> type = getElementType(annotatedElement);

        Enumerated ann = annotatedElement.getAnnotation(Enumerated.class);
        if (ann != null) {
            return ColumnType.VARCHAR;
        }

        if (type.equals(Long.class) || type.equals(Long.TYPE)) {
            return ColumnType.LONG;
        } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            return ColumnType.BOOLEAN;
        } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            return ColumnType.INTEGER;
        } else if (type.equals(Timestamp.class)) {
            return ColumnType.TIMESTAMP;
        } else if (type.equals(String.class)) {
            return ColumnType.VARCHAR;
        } else if (type.isEnum()) {
            return ColumnType.INTEGER;
        } else if (type.isAssignableFrom(Map.class)) {
            return ColumnType.BLOB;
        } else if (type.isAssignableFrom(Double.class) || type.equals(Double.TYPE)) {
            return ColumnType.DOUBLE;
        } else if (type.isAssignableFrom(Date.class)) {
            return ColumnType.DATE;
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static Class<?> getElementType(AnnotatedElement annotatedElement) {
        Class<?> type;
        if (annotatedElement instanceof Field) {
            type = ((Field) annotatedElement).getType();
        } else {
            type = ((Method) annotatedElement).getReturnType();
        }
        return type;
    }

    private static boolean isPropertyAccess(Class clazz) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (columnAnnotated(method)) {
                return true;
            }
        }
        return false;
    }

    private static boolean columnAnnotated(AnnotatedElement element) {
        return element.isAnnotationPresent(Column.class) || element.isAnnotationPresent(Lob.class) || element.isAnnotationPresent(Id.class);
    }

    private static boolean isFieldAccess(Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (columnAnnotated(field)) {
                return true;
            }
        }
        return false;
    }
}
