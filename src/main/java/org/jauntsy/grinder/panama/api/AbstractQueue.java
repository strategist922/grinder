package org.jauntsy.grinder.panama.api;

import backtype.storm.topology.IRichSpout;

import java.io.Serializable;

/**
 * User: ebishop
 * Date: 12/27/12
 * Time: 4:53 PM
 */
public interface AbstractQueue<T> extends Serializable {

    QueueWriter<T> buildWriter();

    IRichSpout buildSpout(String id);

}
