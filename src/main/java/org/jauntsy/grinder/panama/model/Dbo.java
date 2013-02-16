package org.jauntsy.grinder.panama.model;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 12:53 PM
 */
public interface Dbo {

    Dbo append(String column, String value);
    Dbo append(String column, Integer value);
    Dbo append(String column, Long value);
    Dbo append(String column, Float value);
    Dbo append(String column, Double value);
    Dbo append(String column, Boolean value);

    Dbo append(String table, Dbo row);

    Dbo push(String table, List id);
    Dbo push(String table, Object... id);

    Dbo pop();

    String getString(String column);

    Integer getInt(String column);
    Long getLong(String column);
    Float getFloat(String column);
    Double getDouble(String column);

    Boolean getBoolean(String column);

    Dbo getRow(String table, Object... id);
    Dbo getRow(String table, List id);
    Dbo getRow(String table, Dbo id);

    List getId();

    byte[] toBytes();

    void accept(DboVisitor visitor);

}
