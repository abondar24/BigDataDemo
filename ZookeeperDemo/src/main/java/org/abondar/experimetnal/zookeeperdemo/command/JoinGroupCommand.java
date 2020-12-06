package org.abondar.experimetnal.zookeeperdemo.command;


import org.abondar.experimetnal.zookeeperdemo.ConnectionWatcher;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class JoinGroupCommand extends ConnectionWatcher implements Command {


    @Override
    public void execute() {
        try {
            connect(CommandUtil.ZOOKEEPER_HOST);
            String path = "/" + CommandUtil.TEST_GROUP + "/" + CommandUtil.TEST_MEMBER;
            String createdPath = zooKeeper.create(path,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("Created " + createdPath);

            close();
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(1);
        }

    }
}
