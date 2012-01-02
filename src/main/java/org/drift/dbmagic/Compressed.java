package org.drift.dbmagic;

import java.lang.annotation.*;

/**
 * @author Dima Frid
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@Inherited
public @interface Compressed {
}
