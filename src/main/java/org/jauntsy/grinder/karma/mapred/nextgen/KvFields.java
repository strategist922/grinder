package org.jauntsy.grinder.karma.mapred.nextgen;

import backtype.storm.tuple.Fields;
import scala.actors.threadpool.Arrays;

import java.util.List;

import static org.jauntsy.nice.Nice.listof;

/**
 * User: ebishop
 * Date: 2/11/13
 * Time: 10:54 AM
 */
public class KvFields extends Fields {

    private final int keySize;

    public KvFields(int keySize) {
        this(keySize, listof(String.class));
    }

    public KvFields(int keySize, String... fields) {
        this(keySize, Arrays.asList(fields));
    }

    public KvFields(int keySize, List<String> fields) {
        super(fields);
        this.keySize = keySize;
    }
}
