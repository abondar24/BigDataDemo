package org.abondar.experimetnal.zookeeperdemo;


import org.apache.zookeeper.KeeperException;

import java.util.List;

public class DeleteGroup extends ConnectionWatcher {

    public void delete(String groupName) throws Exception{
        String path = "/" + groupName;

        try {
            List<String> children = zooKeeper.getChildren(path, false);
            for (String child : children) {
                zooKeeper.delete(path+"/"+child,-1);
            }
        } catch (KeeperException.NoNodeException ex){
            System.out.printf("Group %s does not exist\n",groupName);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        DeleteGroup group = new DeleteGroup();
        group.connect(args[0]);
        group.delete(args[1]);
        group.close();
    }
}
