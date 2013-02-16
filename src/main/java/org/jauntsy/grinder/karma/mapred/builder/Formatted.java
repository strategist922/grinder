package org.jauntsy.grinder.karma.mapred.builder;

import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.mapred.Formatter;
import org.jauntsy.grinder.karma.mapred.KarmaBuilderContext;

import java.util.List;

/**
 * User: ebishop
 * Date: 1/11/13
 * Time: 12:05 PM
 *
 * TODO: Get rid of zero... I don't think it is needed
 */
public class Formatted implements Buildable {

    private final KarmaBuilderContext context;
    private final Reduced reduced;
    private final List<String> outputValueFields;
    private final Formatter formatter;

    private boolean built = false;

    public Formatted(KarmaBuilderContext context, Reduced reduced, List<String> outputValueFields, Formatter formatter) {
        this.context = context;
        this.reduced = reduced;
        this.outputValueFields = outputValueFields;
        this.formatter = formatter;
    }

    public void buildIfNeeded(KarmaTopologyBuilder builder) {
        if (!built) {
            built = true;
            reduced.build(outputValueFields, formatter);
        }
    }

}
