package org.jauntsy.grinder.karma.mapred.builder;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.karma.mapred.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
* User: ebishop
* Date: 1/7/13
* Time: 1:05 PM
*/
public class ViewBuilder {

    private final KarmaBuilderContext context;
    private final Fields keyFields;
    private final Fields reducerOutputFields;
    private final Reducer reducer;
    private List<Source> sources;
    private final Integer numReducers;
    private final Fields formatterOutputFields;
    private final Formatter formatter;

    public ViewBuilder(KarmaBuilderContext context, Fields keyFields, Fields reducerOutputFields, Reducer reducer, Integer numReducers, Fields formatterOutputFields, Formatter formatter) {
        assert(context != null);
        assert(keyFields != null);
        assert(reducerOutputFields != null);
        assert(reducer != null);
        this.context = context;
        this.keyFields = keyFields;
        this.reducerOutputFields = reducerOutputFields;
        this.reducer = reducer;
        this.sources = new ArrayList<Source>();
        this.numReducers = numReducers;
        this.formatterOutputFields = formatterOutputFields;
        this.formatter = formatter;
    }

    private String buildMapperBoltName(String viewName, Source source) {
        return viewName + "_" + source.sourceComponentId + "_" + source.getSourceName() + "_map";
    }

    public void build(String viewName) {
        Set<String> mapperIds = new HashSet<String>();
        for (Source source : sources) {
            String mapperBoltId = buildMapperBoltName(viewName, source);
            mapperIds.add(mapperBoltId);
            context.getTopologyBuilder()
                    .setBolt(
                            mapperBoltId,
                            new MapperBolt(source.sourceComponentId, source.sourceName, source.keyFields, source.valueFields, viewName, keyFields, reducerOutputFields, source.mapper, reducer, context.buildReducerState()),
                            source.parallelismHint
                    )
                    .fieldsGrouping(source.sourceComponentId, source.keyFields)
                    .directGrouping(viewName, mapperBoltId);
                    //.setMaxSpoutPending(1000); // *** is this my outputs that block or inputs?

        }
        String reducerBoltId = viewName;
        BoltDeclarer reducerBolt = context.getTopologyBuilder().setBolt(
                reducerBoltId,
                new ReducerBolt(viewName, keyFields, reducerOutputFields, reducer, mapperIds, formatterOutputFields, formatter, context.buildReducerState()),
                numReducers
        );
        for (String mapperId : mapperIds) {
            reducerBolt.fieldsGrouping(mapperId, new Fields("key"));
        }

        context.registerOutputIdFieldsForComponent(viewName, keyFields.toList());
    }

    ViewBuilder addSource(Source s) {
        sources.add(s);
        return this;
    }

    public static class Source {

        public String sourceName;
        public String sourceComponentId;
        public Fields keyFields;
        public Fields valueFields;
        public Mapper mapper;

        public Number parallelismHint;

        public Source(String sourceName, String sourceComponentId, List<String> keyFields, List<String> valueFields, Mapper mapper, Number parallelismHint) {
            this(sourceName, sourceComponentId, new Fields(keyFields), new Fields(valueFields), mapper, parallelismHint);
        }

        public Source(String sourceName, String sourceComponentId, Fields keyFields, Fields valueFields, Mapper mapper, Number parallelismHint) {
            if (keyFields == null)
                throw new IllegalArgumentException();
            if (valueFields == null)
                throw new IllegalArgumentException();
            // *** TODO: validate this
            this.sourceName = sourceName;
            this.sourceComponentId = sourceComponentId;
            this.keyFields = keyFields;
            this.valueFields = valueFields;
            this.mapper = mapper;
            this.parallelismHint = parallelismHint;
        }

        public Source parallelismHint(int parallelismHint) {
            this.parallelismHint = parallelismHint;
            return this;
        }

        public void setName(String name) {
            this.sourceName = name;
        }

        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public void setMapper(Mapper mapper) {
            this.mapper = mapper;
        }

        public Mapper getMapper() {
            return mapper;
        }
    }
}
