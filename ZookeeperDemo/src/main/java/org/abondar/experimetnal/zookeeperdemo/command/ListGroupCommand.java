package org.abondar.experimetnal.zookeeperdemo.command;


import org.abondar.experimetnal.zookeeperdemo.ConnectionWatcher;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;

import java.util.List;

public class ListGroupCommand extends ConnectionWatcher implements Command {

    @Override
    public void execute() {
        String path = "/" + ConnectionUtil.TEST_GROUP;

        try {
            connect(ConnectionUtil.HOST);
            List<String> children = zooKeeper.getChildren(path, false);
            if (children.isEmpty()) {
                System.err.printf("No members in group %s\n", ConnectionUtil.TEST_GROUP);
                System.exit(1);
            }

            for (String child : children) {
                System.out.println(child);
            }
            close();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            System.exit(2);
        }
    }

}
