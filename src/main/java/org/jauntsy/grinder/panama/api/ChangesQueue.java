package org.jauntsy.grinder.panama.api;

import backtype.storm.topology.IRichSpout;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 3:02 PM
 */
public interface ChangesQueue extends AbstractQueue {

    ChangesWriter buildWriter();

    IRichSpout buildSpout(String id);

}
