package org.jauntsy.grinder.replay.api;

import java.io.Serializable;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/20/12
 * Time: 2:07 PM
 */
public interface ArchiveConfig extends Serializable {
    public ArchiveClient buildArchive(Map conf);
}
