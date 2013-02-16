package org.jauntsy.nice;

import java.io.File;
import java.io.IOException;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 9:39 AM
 */
public class Files {
    public static File createTmpDir(String prefix) throws IOException {
        File file = File.createTempFile(prefix, "tmp");
        File dir = new File(file.getParentFile(), file.getName() + ".dir");
        dir.mkdirs();
        return dir;
    }
}
