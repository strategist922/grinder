package org.jauntsy.grinder.karma.demo;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.karma.mapred.Crc64;
import org.jauntsy.grinder.karma.mapred.Schema;
import org.jauntsy.grinder.karma.serialization.DefaultSerializer;
import org.jauntsy.grinder.karma.serialization.TupleSerializer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.jauntsy.nice.Nice.L;

/**
* User: ebishop
* Date: 1/16/13
* Time: 9:14 PM
*/
public class JsonProducer {

    public static final Logger LOG = Logger.getLogger(JsonProducer.class);

    public static final TupleScheme ORG_SCHEME = new TupleScheme(new Fields("id", "name"));
    public static final TupleScheme USER_SCHEME = new TupleScheme(new Fields("id", "name", "orgId"));
    public static final JsonScheme TWEET_SCHEME = new JsonScheme(new Fields("id", "tweet"));

    private final Producer<Long, Message> producer;

    private boolean printSendsToConsole = false;

    public JsonProducer(Producer<Long, Message> producer) {
        this.producer = producer;
    }

    public JsonProducer printSendsToConsole(boolean enabled) {
        this.printSendsToConsole = enabled;
        return this;
    }

    Map<String,Schema> SCHEMA_CACHE = new HashMap<String,Schema>();

    void update(String tableSpec, Map mo) {
        sendJson(getSchema(tableSpec), mo);
    }

    void update(String tableSpec, List<String> columns, List values) {
        update(getSchema(tableSpec), columns, values);
    }

    void update(Schema table, List<String> columns, List values) {
        Map mo = new LinkedHashMap();
        for (int i = 0; i < columns.size(); i++) {
            mo.put(columns.get(i), values.get(i));
        }
        sendJson(table, mo);
    }

    void sendJson(Schema table, Map mo) {
        JSONObject jo = new JSONObject();
        List id = new ArrayList();
        jo.putAll(mo);
        for (String idField : table.getIdFields()) {
            Object idPart = jo.get(idField);
            if (idPart == null)
                throw new IllegalArgumentException("Field " + idField + " is required.");
            id.add(idPart);
        }
        if (printSendsToConsole)
            System.out.println("SEND " + table.getName() + " <= " + jo.toJSONString());
        send(table.getName(), id, toBytes(jo));
    }

    private byte[] toBytes(JSONObject jo) {
        return JsonScheme.serialize(jo);
    }

    @Deprecated
    void sendJson(Schema schema, List values) {
        List<String> idFields = schema.getIdFields();
        List<String> valueFields = schema.getValueFields();
        int numberOfRequiredParameters = idFields.size() + valueFields.size();
        if(values.size() != numberOfRequiredParameters)
            throw new IllegalArgumentException("Expected " + numberOfRequiredParameters + " arguments. Got: " + values.size());
        JSONObject jo = new JSONObject();
        List id = new ArrayList();
        Iterator vv = values.iterator();
        for (String idField : idFields) {
            Object idPart = vv.next();
            id.add(idPart);
            jo.put(idField, idPart);
        }
        for (String valueField : valueFields)
            jo.put(valueField, vv.next());
        send(schema.getName(), id, toBytes(jo));
    }

    private Schema getSchema(String schemaSpec) {
        Schema schema = SCHEMA_CACHE.get(schemaSpec);
        if (schema == null) {
            schema = Schema.parse(schemaSpec);
            SCHEMA_CACHE.put(schemaSpec, schema);
        }
        return schema;
    }

    void sendTuple(String topic, int numIdColumns, List value) {
        List id = new ArrayList(numIdColumns);
        for (int i = 0; i < numIdColumns; i++)
            id.add(value.get(i));
        send(topic, id, ser.serialize(value));
    }

    private void send(String topic, List id, byte[] bytes) {
        producer.send(new ProducerData<Long,Message>(
                topic,
                Crc64.getCrc(JSONValue.toJSONString(id).getBytes()), // *** TODO: need a good CRC for keys
                L(new Message(bytes))
        ));
    }

    private static TupleSerializer ser = new DefaultSerializer();

    @Deprecated
    void sendUser(long id, String name, int orgId) throws UnsupportedEncodingException {
        sendTuple("user", 1, L(id, name, orgId));
    }

    @Deprecated
    void deleteUser(long id) throws UnsupportedEncodingException {
        sendTuple("user", 1, L(id, null, null));
    }

    @Deprecated
    void delete(String schema, Map doc) {
        Schema s = Schema.parse(schema);
        Map toSend = new HashMap(doc);
        List<String> idFields = s.getIdFields();
        for (String valueField : s.getValueFields())
            toSend.put(valueField, null);
        sendJson(s, toSend);
    }

    public static class TupleScheme implements Scheme {

        private final Fields fields;

        private transient TupleSerializer ser;

        public TupleScheme(String... fields) {
            this.fields = new Fields(fields);
        }

        public TupleScheme(Fields fields) {
            this.fields = fields;
        }

        @Override
        public List<Object> deserialize(byte[] bytes) {
            if (ser == null)
                ser = new DefaultSerializer();
            return ser.deserialize(bytes);
        }

        @Override
        public Fields getOutputFields() {
            return this.fields;
        }

    }

}
