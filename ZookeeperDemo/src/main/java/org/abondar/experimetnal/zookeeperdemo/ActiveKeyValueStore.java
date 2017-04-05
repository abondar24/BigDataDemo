package org.abondar.experimetnal.zookeeperdemo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class ActiveKeyValueStore extends ConnectionWatcher {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final int MAX_RETRIES = 10;
    private static final int RETRY_PERIOD_SECONDS = 30;

    public void write(String path, String value) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, value.getBytes(CHARSET),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zooKeeper.setData(path, value.getBytes(CHARSET), -1);
        }
    }


    public void writeLoop(String path, String value) throws Exception {
        int retries = 0;
        while (true) {
            try {
                Stat stat = zooKeeper.exists(path, false);
                if (stat == null) {
                    zooKeeper.create(path, value.getBytes(CHARSET),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zooKeeper.setData(path, value.getBytes(CHARSET), -1);
                }
                return;
            } catch (KeeperException.SessionExpiredException ex) {
                System.err.println(ex.getMessage());
            } catch (KeeperException ex) {
                if (retries++ == MAX_RETRIES) {
                    System.err.println(ex.getMessage());
                }

                TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
            }
        }

    }

    public String read(String path, Watcher watcher) throws Exception {
        byte[] data = zooKeeper.getData(path, watcher, null);
        return new String(data, CHARSET);
    }


}
