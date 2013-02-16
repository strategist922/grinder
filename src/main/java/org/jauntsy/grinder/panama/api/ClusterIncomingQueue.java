package org.jauntsy.grinder.panama.api;

/**
 * User: ebishop
 * Date: 12/11/12
 * Time: 6:43 PM
 */
public interface ClusterIncomingQueue extends AbstractQueue<Update> {
    @Override
    ClusterWriter buildWriter();
}
