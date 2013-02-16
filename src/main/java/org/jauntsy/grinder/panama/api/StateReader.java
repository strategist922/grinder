package org.jauntsy.grinder.panama.api;

/**
 * User: ebishop
 * Date: 12/11/12
 * Time: 7:02 PM
 */
public interface StateReader {
    Dbo get(String table, Dbo query);
    void close();
}
