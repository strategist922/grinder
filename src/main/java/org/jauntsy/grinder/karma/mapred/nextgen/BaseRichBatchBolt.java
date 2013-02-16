package org.jauntsy.grinder.karma.mapred.nextgen;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.jauntsy.grinder.karma.mapred.SimpleTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 2/11/13
 * Time: 11:19 AM
 *
 * In essense, a two thread bolt with the main thread collecting tuples
 * and dispatching them to the secondary thread as a batch of tuples.
 *
 * The first batch is always one tuple, the next batch contains
 * any tuples received while processing the first batch, etc. etc. etc.
 *
 * TODO: receive thread currently blocks while the outgoing queue exceeds maxBatchSize.
 * Consider slowing down input until a max threshold of maxQueueSize * 2. My gut tells
 * me this will provide much smoother processing.
 *
 */
public abstract class BaseRichBatchBolt extends BaseRichBolt {

    private final int maxBatchSize;
    private final int maxDelayMs;

    private Object QUEUE_LOCK;
    private List<Tuple> queue;
    private Thread workerThread;
    private long lastBatchTimeMs = 0L;

    public BaseRichBatchBolt(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        this.maxDelayMs = 0;
    }

    public BaseRichBatchBolt(int maxBatchSize, int maxDelayMs) {
        this.maxBatchSize = maxBatchSize;
        this.maxDelayMs = maxDelayMs;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.QUEUE_LOCK = new Object();
        this.queue = new ArrayList<Tuple>();
        this.workerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    List<Tuple> nextBatch = null;
                    synchronized (QUEUE_LOCK) {
                        while (nextBatch == null) {
                            if (maxDelayMs > 0) {
                                long now = System.currentTimeMillis();
                                long timeSinceLastBatch = now - lastBatchTimeMs;
                                if (queue.size() >= maxBatchSize || (queue.size() > 0 && timeSinceLastBatch >= maxDelayMs)) {
                                    nextBatch = queue;
                                    queue = new ArrayList<Tuple>();
                                    QUEUE_LOCK.notify();
                                    lastBatchTimeMs = now;
                                } else {
                                    long maxWaitTime = maxDelayMs - timeSinceLastBatch;
                                    try {
                                        QUEUE_LOCK.wait(maxWaitTime > 0 ? maxWaitTime : 1000);
                                    } catch(InterruptedException ignore) {

                                    }
                                }
                            } else {
                                if (queue.size() > 0) {
                                    nextBatch = queue;
                                    queue = new ArrayList<Tuple>();
                                    QUEUE_LOCK.notify();
                                } else {
                                    try {
                                        QUEUE_LOCK.wait(1000);
                                    } catch(InterruptedException ignore) {

                                    }
                                }
                            }
                        }
                    }
                    if (nextBatch != null) {
                        executeBatch(nextBatch);
                    }
                }
            }
        });
        this.workerThread.setDaemon(true);
        this.workerThread.start();
        this.lastBatchTimeMs = 0L;
    }

    @Override
    public final void execute(Tuple input) {
        synchronized (QUEUE_LOCK) {
            while (queue.size() > maxBatchSize - 1) {
                try {
                    QUEUE_LOCK.wait(1000);
                } catch (InterruptedException ignore) {

                }
            }
            queue.add(input);
            QUEUE_LOCK.notify();
        }
    }

    public abstract void executeBatch(List<Tuple> inputs);

    public static void main(String[] args) {
        Fields fields = new Fields("i");
        BaseRichBatchBolt bolt = new BaseRichBatchBolt(2000, 10000) {
            @Override
            public void executeBatch(List<Tuple> inputs) {
                System.out.println("Batch size: " + inputs.size());
                Utils.sleep((long)(Math.random() * inputs.size() / 10));
            }

            @Override
            public void cleanup() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }
        };
        bolt.prepare(null, null, null);
//        for (int i = 0; i < 1000000; i++) {
//            bolt.execute(new SimpleTuple(fields, new Values(i)));
//            Utils.sleep(2);
//        }
        bolt.execute(new SimpleTuple(fields, new Values(0)));
        Utils.sleep(1000);
        bolt.execute(new SimpleTuple(fields, new Values(1)));
        Utils.sleep(100000);
    }

}
