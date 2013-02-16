package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

public class FailedFetchException extends RuntimeException {
    public FailedFetchException(Exception e) {
        super(e);
    }
}
