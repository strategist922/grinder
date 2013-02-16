package org.jauntsy.grinder.panama.model;

import org.jauntsy.grinder.panama.UniversalComparator;

import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import java.util.Arrays;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 1:05 PM
 */
public class BsonDbo extends DboBase<BsonDbo> {

    BsonDbo parent;
    BSONObject data;

    public BsonDbo(Object... _id) {
        this(Arrays.asList(_id));
    }

    public BsonDbo(List _id) {
        this((BsonDbo)null, _id);
    }

    public BsonDbo(BsonDbo parent, List _id) {
        this.parent = parent;
        this.data = new BasicBSONObject();
        this.data.put("_id", _id);
    }

    public BsonDbo(BsonDbo parent, BSONObject bsonObject) {
        this.parent = parent;
        this.data = bsonObject;
    }

    @Override
    public Dbo push(String table, List id) {
        Dbo child = new BsonDbo(this, id);
        append(table, child);
        return child;
    }

    @Override
    public Dbo pop() {
        return parent;
    }

    @Override
    public List getId() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    protected Object _get(String column) {
        return this.data.get(column);
    }

    @Override
    protected Dbo _getRow(String table, List id) {
        String column = "@" + table;
        BasicBSONList list = (BasicBSONList)this.data.get(column);
        if (list == null)
            return null;
        for (Object o : list) {
            BasicBSONObject that = (BasicBSONObject)o;
            if (0 == UniversalComparator.INSTANCE.compareLists(id, (List)that.get("_id"))) {
                return new BsonDbo(this, that);
            }
        }
        return null;
    }

    @Override
    protected BsonDbo _append(String column, Object value) {
        this.data.put(column, value);
        return this;
    }

    @Override
    protected BsonDbo _append(String table, Dbo row) {
        String column = "@" + table;
        BasicBSONList rr = (BasicBSONList)this.data.get(column);
        if (rr == null) {
            rr = new BasicBSONList();
            this.data.put(column, rr);
        }
        rr.add(((BsonDbo)row).data);
        return this;
    }

    @Override
    public byte[] toBytes() {
        return encoder.encode(data);
    }

    @Override
    public void accept(DboVisitor visitor) {
        for (String column : data.keySet()) {
            if (column.startsWith("@")) {
                List<BSONObject> rows = (List)data.get(column);
                for (BSONObject bso : rows)
                    visitor.acceptRow(column, new BsonDbo(this, bso));
            } else {
                visitor.acceptColumn(column, data.get(column));
            }
        }
    }

    private static DefaultDBEncoder encoder = new DefaultDBEncoder();
    private static DefaultDBDecoder decoder = new DefaultDBDecoder();

    public static BsonDbo fromBytes(byte[] bytes) {
        BasicBSONObject bsonObject = (BasicBSONObject) decoder.readObject(bytes);
        return bsonObject == null ? null : new BsonDbo(null, bsonObject);
    }

    public static class Test extends DboTestBase {

        @Override
        public Dbo newDbo(List id) {
            return new BsonDbo(null, id);
        }

        @Override
        public Dbo fromBytes(byte[] bytes) {
            return BsonDbo.fromBytes(bytes);
        }
    }
}
