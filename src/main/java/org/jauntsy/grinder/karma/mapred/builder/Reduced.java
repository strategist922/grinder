package org.jauntsy.grinder.karma.mapred.builder;

import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.mapred.Formatter;
import org.jauntsy.grinder.karma.mapred.KarmaBuilderContext;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.karma.mapred.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 1/11/13
 * Time: 11:07 AM
 */
public class Reduced implements Buildable {

    private final KarmaBuilderContext context;

    private final Sources sources;
    private final Reducer reducer;

    private final int groupDepth;

    private final String targetStream;
    private final List<String> keyFields;
    private final List<String> valueFields;

    private Integer numReducers;
    private boolean built = false;

    public Reduced(KarmaBuilderContext context, Sources sources, List<String> sourceOutputFieldNames, Reducer reducer, String targetStream, List<String> reducerOutputFieldNames) {
        this.context = context;
        this.sources = sources;
        this.reducer = reducer;

        if (reducerOutputFieldNames.size() > sourceOutputFieldNames.size())
            throw new IllegalArgumentException("Invalid reducer field names");

        this.groupDepth = sourceOutputFieldNames.size() - reducerOutputFieldNames.size();
        for (int i = 0; i < reducerOutputFieldNames.size(); i++) {
            String sourceFieldName = sourceOutputFieldNames.get(groupDepth + i);
            String reduceFieldName = reducerOutputFieldNames.get(i);
            if (!reduceFieldName.equals(sourceFieldName)) {
                throw new IllegalArgumentException("Invalid reduce field name: " + reduceFieldName + ", expected: " + sourceFieldName);
            }
        }

        this.keyFields = new ArrayList<String>();
        keyFields.addAll(sourceOutputFieldNames.subList(0, groupDepth));

        this.valueFields = new ArrayList<String>();
        valueFields.addAll(sourceOutputFieldNames.subList(groupDepth, sourceOutputFieldNames.size()));

        this.targetStream = targetStream;
        if (targetStream == null)
            throw new IllegalArgumentException("target tablename is required");
    }

    public Reduced numReducers(int numReducers) {
        this.numReducers = numReducers;
        return this;
    }

    public void buildIfNeeded(KarmaTopologyBuilder builder) { // *** TODO: this builder param is absurd.
        if (!built) {
            System.out.println("Building " + targetStream);
            built = true;
        ViewBuilder viewBuilder = new ViewBuilder(
                context,
                keyFields == null ? null : new Fields(keyFields),
                valueFields == null ? null : new Fields(valueFields),
                reducer,
                numReducers,
                null,
                null
        );
        for (ViewBuilder.Source s : sources) {
            viewBuilder.addSource(s);
        }
        viewBuilder.build(this.targetStream);
        }
    }

    // *** TODO: sloppy, this is here so other classes can call back in and build, confusing
    void build(List mergeOutputFields, Formatter formatter) {
        ViewBuilder viewBuilder = new ViewBuilder(context, keyFields == null ? null : new Fields(keyFields), valueFields == null ? null : new Fields(valueFields), reducer, numReducers, new Fields(mergeOutputFields), formatter);
        for (ViewBuilder.Source s : sources) {
            viewBuilder.addSource(s);
        }
        viewBuilder.build(this.targetStream);
    }

    public Formatted fmt(String signature, String formatter) {
        return fmt(signature, context.buildFormatter(formatter));
    }

    public Formatted fmt(String signature, Formatter formatter) {
        Schema schema = Schema.parseSignature(signature);
        return context.replace(this, new Formatted(
                context,
                this,
                schema.getValueFields(),
                formatter
        ));
    }

}
