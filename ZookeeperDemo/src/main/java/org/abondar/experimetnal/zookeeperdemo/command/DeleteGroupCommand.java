package org.abondar.experimetnal.zookeeperdemo.command;


import org.abondar.experimetnal.zookeeperdemo.ConnectionWatcher;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;

import java.util.List;

public class DeleteGroupCommand extends ConnectionWatcher implements Command {


    @Override
    public void execute() {
       try {
           connect(CommandUtil.ZOOKEEPER_HOST);
           String path = "/" + CommandUtil.TEST_GROUP;
           List<String> children = zooKeeper.getChildren(path, false);
           for (String child : children) {
               zooKeeper.delete(path+"/"+child,-1);
           }

           close();
       } catch (Exception ex){
           System.err.println(ex.getMessage());
           System.exit(1);
       }
    }
}
