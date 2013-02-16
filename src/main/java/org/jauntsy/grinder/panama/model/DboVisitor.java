package org.jauntsy.grinder.panama.model;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 5:40 PM
 */
public abstract class DboVisitor {

    abstract void acceptColumn(String column, Object value);

    abstract void acceptRow(String column, Dbo value);

}
