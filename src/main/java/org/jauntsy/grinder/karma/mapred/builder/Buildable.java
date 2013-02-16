package org.jauntsy.grinder.karma.mapred.builder;

import org.jauntsy.grinder.karma.KarmaTopologyBuilder;

/**
 * User: ebishop
 * Date: 2/7/13
 * Time: 10:23 AM
 */
public interface Buildable {
    void buildIfNeeded(KarmaTopologyBuilder context);
}
