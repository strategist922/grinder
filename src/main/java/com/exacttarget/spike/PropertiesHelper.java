package com.exacttarget.spike;

import org.json.simple.JSONValue;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.io.*;
import java.util.Properties;

/**
 * User: ebishop
 * Date: 12/7/12
 * Time: 2:57 PM
 */
public class PropertiesHelper {

    public Properties parse(Reader reader) throws IOException {
        Properties p = new Properties();
        p.load(reader);
        return p;
    }

    public Properties parse(File propertiesFile) throws IOException {
        FileReader r = new FileReader(propertiesFile);
        Properties p = parse(r);
        r.close();
        return p;
    }

    public Properties parse(String propertiesPath) throws IOException {
        return parse(new File(propertiesPath));
    }

    public Properties parse(Class resourceRoot, String resourcePath) throws IOException {
        return parse(new InputStreamReader(resourceRoot.getResourceAsStream(resourcePath)));
    }

}
