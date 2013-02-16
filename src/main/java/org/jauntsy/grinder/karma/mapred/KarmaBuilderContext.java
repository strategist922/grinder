package org.jauntsy.grinder.karma.mapred;

import backtype.storm.topology.TopologyBuilder;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.mapred.builder.Buildable;
import org.jauntsy.grinder.karma.scripting.ScriptFactory;
import org.jauntsy.grinder.karma.scripting.groovy.GroovyScriptFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/14/13
 * Time: 1:49 PM
 */
public class KarmaBuilderContext implements ScriptFactory {

    private final KarmaConfig karmaConfig;
    private final KarmaTopologyBuilder karmaTopologyBuilder;
    private final TopologyBuilder topologyBuilder;

    private Map<String,List<String>> outputIdFieldsMap = new HashMap<String,List<String>>();
    private ScriptFactory defaultScriptFactory;

    public KarmaBuilderContext(KarmaConfig karmaConfig, KarmaTopologyBuilder karmaTopologyBuilder, TopologyBuilder builder) {
        this(karmaConfig, karmaTopologyBuilder, new GroovyScriptFactory(), builder);
    }

    public KarmaBuilderContext(KarmaConfig karmaConfig, KarmaTopologyBuilder karmaTopologyBuilder, ScriptFactory defaultScriptFactory, TopologyBuilder builder) {
        this.karmaConfig = karmaConfig;
        this.karmaTopologyBuilder = karmaTopologyBuilder;
        this.defaultScriptFactory = defaultScriptFactory;
        this.topologyBuilder = builder;
    }

    public void registerOutputIdFieldsForComponent(String componentId, List<String> outputIdFields) {
        outputIdFieldsMap.put(componentId, outputIdFields);
    }

    public TopologyBuilder getTopologyBuilder() {
        return topologyBuilder;
    }

    @Deprecated
    public ScriptFactory getScriptFactory() {
        return this;
    }

    @Override
    public Mapper buildMapper(String script) {
        return defaultScriptFactory.buildMapper(script);
    }

    @Override
    public Reducer buildReducer(String script) {
        return defaultScriptFactory.buildReducer(script);
    }

    @Override
    public Formatter buildFormatter(String script) {
        return defaultScriptFactory.buildFormatter(script);
    }

    public ReducerState buildReducerState() {
        return karmaConfig.getReducerState();
    }

    public <T extends Buildable> T replace(Buildable old, T theNew) {
        if (old != null)
            karmaTopologyBuilder.pending.remove(old);
        karmaTopologyBuilder.pending.add(theNew);
        return theNew;
    }
}
