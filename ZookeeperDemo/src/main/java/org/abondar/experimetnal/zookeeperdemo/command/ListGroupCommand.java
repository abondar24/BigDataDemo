package org.abondar.experimetnal.zookeeperdemo.command;


import org.abondar.experimetnal.zookeeperdemo.ConnectionWatcher;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;

import java.util.List;

public class ListGroupCommand extends ConnectionWatcher implements Command {

    @Override
    public void execute() {
        String path = "/" + CommandUtil.TEST_GROUP;

        try {
            connect(CommandUtil.ZOOKEEPER_HOST);
            List<String> children = zooKeeper.getChildren(path, true);
            if (children.isEmpty()) {
                System.err.printf("No members in group %s\n", CommandUtil.TEST_GROUP);
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
