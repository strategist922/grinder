package org.jauntsy.grinder.panama.api;

import com.mongodb.BasicDBObject;
import com.mongodb.DefaultDBEncoder;
import org.bson.BSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import static junit.framework.Assert.assertEquals;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 2:31 PM
 */
public class DboBsonDecoder {

    public Dbo decodeObject(byte[] bytes) {
        DboBsonBuilder callback = new DboBsonBuilder();
        new BasicBSONDecoder().decode(bytes, callback);
        return (Dbo)callback.result;
    }

    private class DboBsonBuilder implements BSONCallback {

        public Stack objects = new Stack();
        public Stack<Runnable> onClose = new Stack();
        private Object result;

        @Override
        public void objectStart() {
            objects.push(new Dbo());
            onClose.push(new Runnable() {
                @Override
                public void run() {
                    result = objects.pop();
                }
            });
        }

        @Override
        public void objectStart(String name) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void objectStart(boolean array) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public Object objectDone() {
            onClose.pop().run();
            return result;
        }

        @Override
        public void reset() {
            objects.clear();
            onClose.clear();
            result = null;
        }

        @Override
        public Object get() {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public BSONCallback createBSONCallback() {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void arrayStart() {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void arrayStart(final String name) {
            assertEquals("_id", name);
            System.out.println("DboBsonDecoder$DboBsonBuilder.arrayStart name: " + name);
            objects.push(new ArrayList());
            onClose.push(new Runnable() {
                @Override
                public void run() {
                    List ll = (List)objects.pop();
                    Dbo dbo = (Dbo)objects.peek();
                    dbo._id = ll;
                }
            });
        }

        @Override
        public Object arrayDone() {
            onClose.pop().run();
            return result;
        }

        @Override
        public void gotNull(String name) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotUndefined(String name) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotMinKey(String name) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotMaxKey(String name) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotBoolean(String name, boolean v) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotDouble(String name, double v) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotInt(String name, int v) {
            System.out.println("DboBsonDecoder$DboBsonBuilder.gotInt name: " + name + ", v: " + v);
            Object container = objects.peek();
            if (container instanceof List) {
                ((List)container).add(v);
            } else {
                ((Dbo)container).put(name, v);
            }
        }

        @Override
        public void gotLong(String name, long v) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotDate(String name, long millis) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotString(String name, String v) {
            System.out.println("DboBsonDecoder$DboBsonBuilder.gotString name: " + name + ", v: " + v);
            Object container = objects.peek();
            if (container instanceof List) {
                ((List)container).add(v);
            } else {
                ((Dbo)container).put(name, v);
            }
        }

        @Override
        public void gotSymbol(String name, String v) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotRegex(String name, String pattern, String flags) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotTimestamp(String name, int time, int inc) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotObjectId(String name, ObjectId id) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotDBRef(String name, String ns, ObjectId id) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotBinaryArray(String name, byte[] data) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotBinary(String name, byte type, byte[] data) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotUUID(String name, long part1, long part2) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotCode(String name, String code) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void gotCodeWScope(String name, String code, Object scope) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }
    }

    public static void main(String[] args) {
        BasicDBObject joe = new BasicDBObject("_id", Arrays.asList("acme", 7)).append("name", "Joe").append("age", 27);
        byte[] bytes = new DefaultDBEncoder().encode(joe);
        System.out.println(new DboBsonDecoder().decodeObject(bytes).toJsonString(true));
    }
}
