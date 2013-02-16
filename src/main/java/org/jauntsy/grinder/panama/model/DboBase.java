package org.jauntsy.grinder.panama.model;

import java.util.Arrays;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 1:05 PM
 */
public abstract class DboBase<T extends DboBase> implements Dbo {

    protected abstract Object _get(String column);

    protected abstract Dbo _getRow(String column, List id);

    protected abstract T _append(String column, Object value);

    protected abstract T _append(String table, Dbo row);

    @Override
    public Dbo append(String column, String value) {
        return _append(column, value);
    }

    @Override
    public Dbo append(String column, Integer value) {
        return _append(column, value);
    }

    @Override
    public Dbo append(String column, Long value) {
        return _append(column, value);
    }

    @Override
    public Dbo append(String column, Float value) {
        return _append(column, value);
    }

    @Override
    public Dbo append(String column, Double value) {
        return _append(column, value);
    }

    @Override
    public Dbo append(String column, Boolean value) {
        return _append(column, value);
    }

    @Override
    public Dbo append(String table, Dbo row) {
        return _append(table, row);
    }

    @Override
    public String getString(String column) {
        return (String)_get(column);
    }

    @Override
    public Integer getInt(String column) {
        Number n = (Number)_get(column);
        return n == null ? null : n instanceof Integer ? (Integer)n : n.intValue();
    }

    @Override
    public Long getLong(String column) {
        Number n = (Number)_get(column);
        return n == null ? null : n instanceof Long ? (Long)n : n.longValue();
    }

    @Override
    public Float getFloat(String column) {
        Number n = (Number)_get(column);
        return n == null ? null : n instanceof Float ? (Float)n : n.floatValue();
    }

    @Override
    public Double getDouble(String column) {
        Number n = (Number)_get(column);
        return n == null ? null : n instanceof Double ? (Double)n : n.doubleValue();
    }

    @Override
    public Boolean getBoolean(String column) {
        return (Boolean)_get(column);
    }

    @Override
    public Dbo getRow(String table, Object... id) {
        return _getRow(table, Arrays.asList(id));
    }

    @Override
    public Dbo getRow(String table, List id) {
        return _getRow(table, id);
    }

    @Override
    public Dbo getRow(String table, Dbo id) {
        return _getRow(table, id.getId());
    }

    @Override
    public Dbo push(String table, Object... id) {
        return push(table, Arrays.asList(id));
    }
}
