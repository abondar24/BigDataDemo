package org.abondar.experimetnal.zookeeperdemo;


import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

public class ConnectionWatcher implements Watcher {
    private static final int SESSION_TIMEOUT = 5000;
    protected ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect(String hosts) throws Exception {
        zooKeeper = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    public void create(String groupName) throws Exception {
        String path = "/" + groupName;
        String createdPath = zooKeeper.create(path,null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("Created "+createdPath);

    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

}
