package org.drift.dbmagic.utils;

import com.sun.beans.ObjectHandler;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author  Dima Frid
 */
public class ReflectionUtils {
      public static Collection<Field> getAllFields(Class clazz) {
        return getAllFields(clazz, Object.class);
    }

    public static Collection<Field> getAllFields(Class clazz, Class stopClass) {
        List<Field> allFields = new ArrayList<Field>();
        Class current = clazz;
        while (current != stopClass) {
            allFields.addAll(Arrays.asList(current.getDeclaredFields()));
            current = current.getSuperclass();
        }
        return allFields;
    }
    public static boolean isNumeric(Class type) {
        return Number.class.isAssignableFrom(typeToClass(type));
    }

    public static Class typeToClass(Class type) {
        return type.isPrimitive() ? ObjectHandler.typeNameToClass(type.getName()) : type;
    }
}
