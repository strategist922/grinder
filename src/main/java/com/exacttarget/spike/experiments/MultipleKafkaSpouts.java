package com.exacttarget.spike.experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.replay.KafkaSpoutExample;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.*;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/18/12
 * Time: 12:02 PM
 *
 *
 * what does builder.setSpout("myspout", kafkaSpout, 4) do? Ie.
 *
 * conclusion
 *
 * Each spout claims roughly 1/4 the partitions and processes their share
 *
 * Not sure what happens if one spout is unavailable....
 *
 */
public class MultipleKafkaSpouts {
    public static void main(String[] args) throws IOException {
        LocalCluster cluster = new LocalCluster();

        LocalKafkaBroker broker = new LocalKafkaBroker.Builder()
                .brokerid(0)
                .numPartitions(4)
                .enableZookeeper(true)
                .zkConnect("localhost:2000")
                .buildLocalKafkaBroker();

        Producer<Long,Message> producer = broker.buildSyncProducer();
        for (int i = 0; i < 20; i++) {
            String msg = "Message " + i;
            System.out.println("Sending " + msg);
            producer.send(new ProducerData<Long, Message>("test", new Message(msg.getBytes("UTF-8"))));
        }

        TopologyBuilder builder = new TopologyBuilder();
        SpoutConfig spoutConfig = new SpoutConfig(
                KafkaConfig.StaticHosts.fromHostString(Arrays.asList(new String[]{"localhost:9090"}), 4),
                "test",
                "/storm-kafka",
                "test-spout-a"
        );
        spoutConfig.forceFromStart = true;
        spoutConfig.kafkaScheme = new KafkaScheme(new StringScheme());
        builder.setSpout("spout", new KafkaSpout(spoutConfig) {
            @Override
            public void open(Map conf, TopologyContext context, final SpoutOutputCollector collector) {
                super.open(conf, context, new SpoutOutputCollector(new ISpoutOutputCollector() {
                    @Override
                    public List<Integer> emit(String stream, List<Object> tuple, Object id) {
                        System.out.println("!!! Spouts.emit thread: " + Thread.currentThread().getName() + ", stream: " + stream + ", tuple: " + tuple);
                        return collector.emit(stream, tuple, id);
                    }

                    @Override
                    public void emitDirect(int i, String s, List<Object> objects, Object o) {
                        collector.emitDirect(i, s, objects, o);
                    }

                    @Override
                    public void reportError(Throwable throwable) {
                        collector.reportError(throwable);
                    }
                }));
            }
        }, 4);
        builder.setBolt("bolt", new KafkaSpoutExample.DebugBolt("bolt"),4).shuffleGrouping("spout");

        cluster.submitTopology("topo", new Config(), builder.createTopology());

        Utils.sleep(20000);
    }
}
