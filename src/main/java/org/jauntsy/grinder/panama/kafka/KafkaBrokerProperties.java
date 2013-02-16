package org.jauntsy.grinder.panama.kafka;

import org.jauntsy.nice.PropertiesBuilder;

/**
 * User: ebishop
 * Date: 12/18/12
 * Time: 10:03 AM
 */
public class KafkaBrokerProperties<T extends KafkaBrokerProperties> extends PropertiesBuilder<T> {

    public static final String BROKERID = "brokerid";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String NUM_THREADS = "num.threads";
    public static final String SOCKET_SEND_BUFFER = "socket.send.buffer";
    public static final String SOCKET_RECEIVE_BUFFER = "socket.receive.buffer";
    public static final String MAX_SOCKET_REQUEST_BYTES = "max.socket.request.bytes";
    public static final String LOG_DIR = "log.dir";
    public static final String NUM_PARTITIONS = "num.partitions";
    public static final String TOPIC_PARTITION_COUNT_MAP = "topic.partition.count.map";
    public static final String LOG_FLUSH_INTERVAL = "log.flush.interval";
    public static final String LOG_DEFAULT_FLUSH_INTERVAL_MS = "log.default.flush.interval.ms";
    public static final String TOPIC_FLUSH_INTERVALS_MS = "topic.flush.intervals.ms";
    public static final String LOG_DEFAULT_FLUSH_SCHEDULER_INTERVAL_MS = "log.default.flush.scheduler.interval.ms";
    public static final String LOG_RETENTION_HOURS = "log.retention.hours";
    public static final String LOG_RETENTION_SIZE = "log.retention.size";
    public static final String LOG_FILE_SIZE = "log.file.size";
    public static final String LOG_CLEANUP_INTERVAL_MINS = "log.cleanup.interval.mins";
    public static final String ENABLE_ZOOKEEPER = "enable.zookeeper";
    public static final String ZK_CONNECT = "zk.connect";
    public static final String ZK_CONNECTIONTIMEOUT_MS = "zk.connectiontimeout.ms";

    /*
            # The id of the broker. This must be set to a unique integer for each broker.
             */
    public T brokerid(int brokerid) {
        return append(BROKERID, brokerid);
    }

    /*
    # Hostname the broker will advertise to consumers. If not set, kafka will use the value returned
    # from InetAddress.getLocalHost().  If there are multiple interfaces getLocalHost
    # may not be what you want.
     */
    public T hostname(String hostname) {
        return append(HOSTNAME, hostname);
    }

    /*
    # The port the socket server listens on
     */
    public T port(int port) {
        return append(PORT, String.valueOf(port));
    }

    /*
    # The number of processor threads the socket server uses for receiving and answering requests.
    # Defaults to the number of cores on the machine
     */
    public T numThreads(int numThreads) {
        return append(NUM_THREADS, numThreads);
    }

    /*
    # The send buffer (SO_SNDBUF) used by the socket server
     */
    public T socketSendBuffer(int value) {
        return append(SOCKET_SEND_BUFFER, value);
    }

    /*
    # The receive buffer (SO_RCVBUF) used by the socket server
     */
    public T socketReceiveBuffer(int value) {
        return append(SOCKET_RECEIVE_BUFFER, value);
    }

    /*
    # The maximum size of a request that the socket server will accept (protection against OOM)
     */
    public T maxSocketRequestBytes(int value) {
        return append(MAX_SOCKET_REQUEST_BYTES, value);
    }

    /*
    # The directory under which to store log files
     */
    public T logDir(String value) {
        return append(LOG_DIR, value);
    }

    /*
    # The number of logical partitions per topic per server. More partitions allow greater parallelism
    # for consumption, but also mean more files.
     */
    public T numPartitions(int value) {
        return append(NUM_PARTITIONS, value);
    }

    /*
    # Overrides for for the default given by num.partitions on a per-topic basis
    # topic.partition.count.map=topic1:3, topic2:4
     */
    public T topicPartitionCountMap(String value) {
        return append(TOPIC_PARTITION_COUNT_MAP, value);
    }

    /*
    # The number of messages to accept before forcing a flush of data to disk
    # log.flush.interval=10000
     */
    public T logFlushInterval(int value) {
        return append(LOG_FLUSH_INTERVAL, value);
    }

    /*
    # The maximum amount of time a message can sit in a log before we force a flush
    # log.default.flush.interval.ms=1000
     */
    public T logDefaultFlushIntervalMs(long value) {
        return append(LOG_DEFAULT_FLUSH_INTERVAL_MS, value);
    }

    /*
    # Per-topic overrides for log.default.flush.interval.ms
    # topic.flush.intervals.ms=topic1:1000, topic2:3000
     */
    public T topicFlushIntervalsMs(long value) {
        return append(TOPIC_FLUSH_INTERVALS_MS, value);
    }

    /*
    # The interval (in ms) at which logs are checked to see if they need to be flushed to disk.
    # log.default.flush.scheduler.interval.ms=1000
     */
    public T logDefaultFlushSchedulerIntervalMs(long value) {
        return append(LOG_DEFAULT_FLUSH_SCHEDULER_INTERVAL_MS, value);
    }

    /*
    # The minimum age of a log file to be eligible for deletion
    # log.retention.hours=168
     */
    public T logRetentionHours(int value) {
        return append(LOG_RETENTION_HOURS, value);
    }

    /*
    # A size-based retention policy for logs. Segments are pruned from the log as long as the remaining
    # segments don't drop below log.retention.size.
    # log.retention.size=1073741824
     */
    public T logRetentionSize(long value) {
        return append(LOG_RETENTION_SIZE, value);
    }

    /*
    # The maximum size of a log segment file. When this size is reached a new log segment will be created.
    # log.file.size=536870912
     */
    public T logFileSize(long value) {
        return append(LOG_FILE_SIZE, value);
    }

    /*
    # The interval at which log segments are checked to see if they can be deleted according
    # to the retention policies
    # log.cleanup.interval.mins=1
     */
    public T logCleanupIntervalMins(int value) {
        return append(LOG_CLEANUP_INTERVAL_MINS, value);
    }

    /*
    # Enable connecting to zookeeper
    # enable.zookeeper=true
     */
    public T enableZookeeper(boolean value) {
        return append(ENABLE_ZOOKEEPER, value);
    }

    /*
    # Zk connection string (see zk docs for details).
    # This is a comma separated host:port pairs, each corresponding to a zk
    # server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002".
    # You can also append an optional chroot string to the urls to specify the
    # root directory for all kafka znodes.
    # zk.connect=localhost:2181
     */
    public T zkConnect(String value) {
        append(ENABLE_ZOOKEEPER, true);
        append(ZK_CONNECT, value);
        return self();
    }

    /*
    # Timeout in ms for connecting to zookeeper
    # zk.connectiontimeout.ms=1000000
     */
    public T zkConnectiontimeoutMs(long value) {
        return append(ZK_CONNECTIONTIMEOUT_MS, value);
    }

    public static class Builder extends KafkaBrokerProperties<Builder> {
        public KafkaBrokerProperties buildKafkaBrokerProperties() {
            KafkaBrokerProperties ret = new KafkaBrokerProperties();
            ret.putAll(this);
            return ret;
        }
    }


}
