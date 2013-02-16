package org.jauntsy.grinder.panama.testing;

import org.jauntsy.grinder.panama.api.Dbv;
import org.jauntsy.grinder.panama.base.BaseSnapshotAdapter;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 4:22 PM
 */
public class LevelDbSnapshotAdapter extends BaseSnapshotAdapter {

    @Override
    public void close() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public List<Dbv> getBatch(String table, List<List> ids) {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public void putBatch(String table, List<List> ids, List<Dbv> values) {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }
}
