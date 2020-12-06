package org.abondar.experimetnal.zookeeperdemo.command;

import org.abondar.experimetnal.zookeeperdemo.ConnectionWatcher;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;

public class CreateGroupCommand  implements Command {


    public void execute() {
        try {
            ConnectionWatcher watcher = new ConnectionWatcher();
            watcher.connect(ConnectionUtil.HOST);
            watcher.create(ConnectionUtil.TEST_GROUP);
            watcher.close();
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(1);
        }

    }
}
