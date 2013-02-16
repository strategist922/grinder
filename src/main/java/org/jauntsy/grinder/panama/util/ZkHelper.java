package org.jauntsy.grinder.panama.util;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: ebishop
 * Date: 9/13/12
 * Time: 12:52 PM
 */
public class ZkHelper {
    public static ZooKeeper connect(String hostPort, int sessionTimeout) throws IOException {
        final AtomicBoolean connected = new AtomicBoolean(false);
        ZooKeeper zk = new ZooKeeper(hostPort, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                synchronized (connected) {
                    connected.set(true);
                    connected.notify();
                }
            }
        });
        synchronized (connected) {
            while (!connected.get()) {
                try {
                    connected.wait(100);
                } catch (Exception ignore) {
                }
            }
        }
        return zk;
    }


    public static void dump(ZooKeeper zk, String path) throws InterruptedException, KeeperException {
        System.out.println(path);
        byte[] data = zk.getData(path, false, null);
        if (data != null && data.length > 0) {
            Object o = toObject(data);
            try { if (o != null) System.out.println("\t" + toString(o)); } catch(Exception ex) {
                if (o != null) System.out.println("\t" + o);
            }

        }
        List<String> children  = zk.getChildren(path.equals("/") ? "/" : path, false);
        for (String child : children) {
            String childPath = path.equals("/") ? path + child : path + "/" + child;
//            System.out.println(childPath);
            dump(zk, childPath);
        }
    }

    public static void dumpToConsole(ZooKeeper zk) throws InterruptedException, KeeperException {
        dumpToConsole(zk, "/");
    }

    public static void dumpToConsole(ZooKeeper zk, String path) throws InterruptedException, KeeperException {
        dump2(zk, path, 0);
    }

    private static void dump2(ZooKeeper zk, String path, int depth) throws InterruptedException, KeeperException {
//        System.out.println(path);
        String[] pathElements = path.split("/");
        if (pathElements.length == 0)
            pathElements = new String[] { "" };
        System.out.println(indent(depth) + "/" + pathElements[pathElements.length - 1]);
        byte[] data = zk.getData(path, false, null);
        if (data != null && data.length > 0) {
            Object o = toObject(data);
            try { if (o != null) System.out.println(indent(depth + 1) + toString(o)); } catch(Exception ex) {
                if (o != null) System.out.println(indent(depth + 1) + o);
            }

        }
        List<String> children  = zk.getChildren(path.equals("/") ? "/" : path, false);
        for (String child : children) {
            String childPath = path.equals("/") ? path + child : path + "/" + child;
//            System.out.println(childPath);
            dump2(zk, childPath, depth + 1);
        }
    }

    private static String indent(int depth) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < depth; i++)
            sb.append("  ");
        return sb.toString();
    }

    private static Object toObject(byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            return new ObjectInputStream(new ByteArrayInputStream(data)).readObject();
        } catch(Exception ex) {
            //System.out.println(ex);
        }
        if (data != null) return new String(data);
        else return null;
    }

    private static String toString(Object o) throws IOException {
        Set<Integer> parsed = new HashSet();
        StringBuilder sb = new StringBuilder();
        toString(parsed, sb, o);
        return sb.toString();
    }

    private static void toString(Set<Integer> parsed, StringBuilder sb, Object o) throws IOException {
//        sb.append(new ObjectMapper().writeValueAsString(o));
        sb.append(JSONValue.toJSONString(o));
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = ZkHelper.connect("166.78.4.30:2181", 10000);
        dump(zk, "/");
    }

}
