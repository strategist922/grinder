package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

/**
 * User: ebishop
 * Date: 12/18/12
 * Time: 4:44 PM
 */
public class HostPortPartition {
    public HostPort hostPort;
    public int partition;
    public HostPortPartition(HostPort hostPort, int partition) {
        this.hostPort = hostPort;
        this.partition = partition;
    }
}
