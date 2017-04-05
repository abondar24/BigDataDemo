package org.abondar.experimetnal.zookeeperdemo;


import org.apache.zookeeper.KeeperException;

import java.util.List;

public class ListGroup extends ConnectionWatcher {

    public void list(String groupName) throws Exception {
        String path = "/" + groupName;

        try {
            List<String> children = zooKeeper.getChildren(path, false);
            if (children.isEmpty()) {
                System.out.printf("No members in group %s\n", groupName);
                System.exit(1);
            }

            for (String child : children) {
                System.out.println(child);
            }
        } catch (KeeperException.NoNodeException ex){
            System.out.printf("Group %s does not exist\n",groupName);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        ListGroup group = new ListGroup();
        group.connect(args[0]);
        group.list(args[1]);
        group.close();
    }
}
