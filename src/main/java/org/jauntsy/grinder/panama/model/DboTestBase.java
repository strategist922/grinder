package org.jauntsy.grinder.panama.model;

import junit.framework.TestCase;

import java.util.*;

import static org.jauntsy.grinder.panama.util.Sweetie.*;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 12:54 PM
 */
public abstract class DboTestBase extends TestCase {

    public void testAll() {
        Dbo joe = newDbo("acme", 7)
                .append("name", "Joe")
                .append("age", 27)
                .append("addresses", newDbo("home")
                        .append("city", "San Francisco")
                        .append("state", "CA")
                )
                .append("foo", "bar");

        assertEquals("Joe", joe.getString("name"));
        assertEquals(27, (int)joe.getInt("age"));
        Dbo home = joe.getRow("addresses", "home");
        assertEquals("San Francisco", home.getString("city"));
        assertEquals("CA", home.getString("state"));

        byte[] bytes = joe.toBytes();
        Dbo joe2 = fromBytes(bytes);

        checkValues(joe, joe2);

        Dbo joe3 = newDbo("acme", 7)
                .append("name", "Joe")
                .append("age", 27)
                .push("addresses", "home")
                .append("city", "San Francisco")
                .append("state", "CA")
                .pop()
                .append("foo", "bar");
        checkValues(joe, joe3);

        final Map todo = map("name", "Joe", "age", 27, "city", "San Francisco", "state", "CA", "foo", "bar");
        joe3.accept(new DboVisitor() {
            public void acceptColumn(String column, Object value) {
                System.out.println("DboTestBase.acceptColumn column: " + column + ", value: " + value);
                if ("_id".equals(column)) {
                    // ignore
                } else if (todo.containsKey(column) && todo.get(column).equals(value))
                    todo.remove(column);
                else
                    fail();
            }

            public void acceptRow(String table, Dbo row) {
                row.accept(this);
            }
        });
        if (todo.size() > 0)
            fail();
    }

    private void checkValues(Dbo expected, Dbo actual) {
        assertEquals(expected.getString("name"), actual.getString("name"));
        assertEquals(expected.getInt("age"), actual.getInt("age"));
        Dbo expectedHome = expected.getRow("addresses", "home");
        Dbo actualHome = actual.getRow("addresses", "home");
        assertEquals(expected.getString("city"), actual.getString("city"));
        assertEquals(expected.getString("state"), actual.getString("state"));
        assertEquals(expected.getString("foo"), actual.getString("foo"));
    }

    public Dbo newDbo(Object... id) {
        return newDbo(Arrays.asList(id));
    }

    public abstract Dbo newDbo(List id);

    public abstract Dbo fromBytes(byte[] bytes);

}
