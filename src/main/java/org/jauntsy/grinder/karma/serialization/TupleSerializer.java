package org.jauntsy.grinder.karma.serialization;

import java.util.List;

/**
 * User: ebishop
 * Date: 1/8/13
 * Time: 11:18 AM
 */
public interface TupleSerializer {
    byte[] serialize(List tuple);
    List deserialize(byte[] bytes);
}
