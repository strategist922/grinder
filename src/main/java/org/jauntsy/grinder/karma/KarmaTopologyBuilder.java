package org.jauntsy.grinder.karma;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.*;
import org.jauntsy.grinder.karma.mapred.*;
import org.jauntsy.grinder.karma.mapred.builder.*;
import org.jauntsy.grinder.karma.operations.map.SelectAll;
import org.jauntsy.grinder.replay.api.ReplaySpoutConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: ebishop
 * Date: 1/3/13
 * Time: 2:24 PM
 */
public class KarmaTopologyBuilder {

    private final KarmaConfig karma;
    private final String topologyNs;
    private final KarmaBuilderContext context;
    private final TopologyBuilder builder;
    public final Set<Buildable> pending = new HashSet<Buildable>();

    public KarmaTopologyBuilder(KarmaConfig karma, String topologyNs) {
        this.karma = karma;
        this.topologyNs = topologyNs;
        this.builder = new TopologyBuilder();
        this.context = new KarmaBuilderContext(karma, this, builder);
    }

    public Mapped map(String values, String schema, String mapper) {
        return map(values, schema, context.buildMapper(mapper));
    }

    public Mapped map(String values, String schema, Mapper mapper) {
        Schema query = Schema.parseSignature(values);
        Schema table = Schema.parse(schema);
        //Schema src = context.parseInputSchema(srcStream, query);
        return this.map(table.getName(), table.getIdFields(), query.getValueFields(), mapper);
    }

    public Mapped map(String srcComponentId, List<String> docIdFields, List<String> docValueFields, Mapper mapper) {
        return new Mapped(
                context,
                null,
                srcComponentId,
                docIdFields,
                docValueFields,
                mapper,
                null
        );
    }


    public Mapped map(String projection, String schema) {
        return map(projection, schema, new SelectAll());
    }

    public void setSpout(String spoutName, String topic, Scheme scheme) {

    }

    public SpoutDeclarer setSpout(String spoutName, String topic, Scheme scheme, int parallelismHint) {
        return builder.setSpout(
                spoutName,
                new KafkaSpout(
                        new ReplaySpoutConfig(
                                karma.getReplayConfig(),
                                topic,
                                scheme,
                                karma.getNs() + "_" + topologyNs + "_" + spoutName
                        )
                ),
                parallelismHint
        );
    }

    public BoltDeclarer setBolt(String id, IRichBolt bolt) {
        return builder.setBolt(id, bolt);
    }

    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        return builder.setBolt(id, bolt, parallelism_hint);
    }

    public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
        return builder.setBolt(id, bolt);
    }

    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
        return builder.setBolt(id, bolt, parallelism_hint);
    }

    public StormTopology createTopology() {
        for (Buildable b : pending) {
            b.buildIfNeeded(this);
        }
        return builder.createTopology();
    }

    public SpoutDeclarer setSpout(String id, IRichSpout spout) {
        return builder.setSpout(id, spout);
    }

    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
        return builder.setSpout(id, spout, parallelism_hint);
    }

}

