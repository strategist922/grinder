package org.jauntsy.grinder.karma.mapred;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.List;

/**
* User: ebishop
* Date: 1/7/13
* Time: 1:04 PM
*/
public class SimpleTuple implements Tuple {

    private Fields fields;
    private List values;

    public SimpleTuple(Fields fields, List values) {
        this.fields = fields;
        this.values = values;
//        if (values == null)
//            throw new IllegalArgumentException();
    }

    @Override
    public int size() {
        return fields.size();
    }

    @Override
    public int fieldIndex(String field) {
        return fields.fieldIndex(field);
    }

    @Override
    public boolean contains(String field) {
        return fields.contains(field);
    }

    @Override
    public Object getValue(int i) {
        return values.get(i);
    }

    @Override
    public String getString(int i) {
        return (String)values.get(i);
    }

    @Override
    public Integer getInteger(int i) {
        Number n = (Number)values.get(i);
        return n == null ? null : n instanceof Integer ? (Integer)n : n.intValue();
    }

    @Override
    public Long getLong(int i) {
        Number n = (Number)values.get(i);
        return n == null ? null : n instanceof Long ? (Long)n : n.longValue();
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean)values.get(i);
    }

    @Override
    public Short getShort(int i) {
        Number n = (Number)values.get(i);
        return n == null ? null : n instanceof Short ? (Short)n : n.shortValue();
    }

    @Override
    public Byte getByte(int i) {
        Number n = (Number)values.get(i);
        return n == null ? null : n instanceof Byte ? (Byte)n : n.byteValue();
    }

    @Override
    public Double getDouble(int i) {
        Number n = (Number)values.get(i);
        return n == null ? null : n instanceof Double ? (Double)n : n.doubleValue();
    }

    @Override
    public Float getFloat(int i) {
        Number n = (Number)values.get(i);
        return n == null ? null : n instanceof Float ? (Float)n : n.floatValue();
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[])values.get(i);
    }

    @Override
    public Object getValueByField(String field) {
        return getValue(fields.fieldIndex(field));
    }

    @Override
    public String getStringByField(String s) {
        return getString(fieldIndex(s));
    }

    @Override
    public Integer getIntegerByField(String s) {
        return getInteger(fieldIndex(s));
    }

    @Override
    public Long getLongByField(String s) {
        return getLong(fieldIndex(s));
    }

    @Override
    public Boolean getBooleanByField(String s) {
        return getBoolean(fieldIndex(s));
    }

    @Override
    public Short getShortByField(String s) {
        return getShort(fieldIndex(s));
    }

    @Override
    public Byte getByteByField(String s) {
        return getByte(fieldIndex(s));
    }

    @Override
    public Double getDoubleByField(String s) {
        return getDouble(fieldIndex(s));
    }

    @Override
    public Float getFloatByField(String s) {
        return getFloat(fieldIndex(s));
    }

    @Override
    public byte[] getBinaryByField(String s) {
        return getBinary(fieldIndex(s));
    }

    @Override
    public List<Object> getValues() {
        return values;
    }

    @Override
    public Fields getFields() {
        return fields;
    }

    @Override
    public List<Object> select(Fields fields) {
        List ret = new ArrayList();
        for (String field : fields)
            ret.add(getValueByField(field));
        return ret;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public String getSourceComponent() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public int getSourceTask() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public String getSourceStreamId() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public MessageId getMessageId() {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    public void setValues(List values) {
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(" ");
            sb.append(":").append(fields.get(i)).append(" ");
            Object value = values.get(i);
            if (value instanceof String) {
                sb.append("'").append(JSONValue.escape((String)value)).append("'");
            } else {
                sb.append(value);
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
