package org.abondar.experimetnal.zookeeperdemo;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class JoinGroup extends ConnectionWatcher {

    public void join (String groupName, String memberName) throws Exception{
        String path = "/" + groupName + "/" + memberName;
        String createdPath = zooKeeper.create(path,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Created " + createdPath);
    }

    public static void main(String[] args) throws Exception {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect(args[0]);
        joinGroup.join(args[1],args[2]);

        //alive till kill or interrupt
        Thread.sleep(Long.MAX_VALUE);
    }
}
