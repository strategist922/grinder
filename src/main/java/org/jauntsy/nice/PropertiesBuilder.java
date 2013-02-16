package org.jauntsy.nice;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * User: ebishop
 * Date: 12/18/12
 * Time: 9:59 AM
 */
public class PropertiesBuilder<T> extends Properties {

    protected PropertiesBuilder() {
        Properties defaults = new Properties();
        putDefaultProperties(defaults);
        putAll(defaults);
    }

    protected T append(String property, Object value) {
        put(property, String.valueOf(value));
        return self();
    }

    public T appendAll(Properties other) {
        putAll(other);
        return self();
    }

    protected T self() {
        return (T)this;
    }

    protected void putDefaultProperties(Properties defaults) {

    }

    public String getString(String property) {
        return (String)get(property);
    }

    public String getString(String property, String def) {
        String s = getString(property);
        return s == null || s.trim().isEmpty() ? def : s;
    }

    public Integer getInteger(String property) {
        return containsKey(property) ? Integer.parseInt(getString(property)) : null;
    }

}
