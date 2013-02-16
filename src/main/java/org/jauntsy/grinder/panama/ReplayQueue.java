package org.jauntsy.grinder.panama;

import backtype.storm.topology.IRichSpout;

/**
 * User: ebishop
 * Date: 12/13/12
 * Time: 3:12 PM
 */
public interface ReplayQueue {
    IRichSpout getSpout();
}
