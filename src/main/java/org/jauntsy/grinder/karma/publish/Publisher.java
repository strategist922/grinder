package org.jauntsy.grinder.karma.publish;

import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.karma.contrib.StateBolt;

/**
* User: ebishop
* Date: 2/14/13
* Time: 3:27 PM
*/
public interface Publisher {
    StateBolt newBolt(String table, Fields idFields);
}
