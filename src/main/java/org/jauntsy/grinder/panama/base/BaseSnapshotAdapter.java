package org.jauntsy.grinder.panama.base;

import org.jauntsy.grinder.panama.api.Dbv;
import org.jauntsy.grinder.panama.api.SnapshotAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 4:23 PM
 */
public abstract class BaseSnapshotAdapter implements SnapshotAdapter {

    @Override
    public void putOne(String table, List id, Dbv value) {
        putBatch(table, list(id), list(value));
    }

    @Override
    public Dbv getOne(String table, List id) {
        return getBatch(table, list(id)).get(0);
    }

    private <T> List<T> list(T... items) {
        List<T> l = new ArrayList<T>();
        for (T t : items)
            l.add(t);
        return l;
    }

}
