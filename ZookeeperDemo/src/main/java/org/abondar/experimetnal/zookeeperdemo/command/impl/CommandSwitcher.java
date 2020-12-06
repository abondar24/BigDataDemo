package org.abondar.experimetnal.zookeeperdemo.command.impl;

import org.abondar.experimetnal.zookeeperdemo.command.ConfigUpdaterCommand;
import org.abondar.experimetnal.zookeeperdemo.command.ConfigWatcherCommand;
import org.abondar.experimetnal.zookeeperdemo.command.DeleteGroupCommand;
import org.abondar.experimetnal.zookeeperdemo.command.CreateGroupCommand;
import org.abondar.experimetnal.zookeeperdemo.command.JoinGroupCommand;
import org.abondar.experimetnal.zookeeperdemo.command.ListGroupCommand;

public class CommandSwitcher {

    private final CommandExecutor executor;

    public CommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public void executeCommand(String cmd){
        try {
            switch (Commands.valueOf(cmd)){
                case CGC:
                    CreateGroupCommand cgc = new CreateGroupCommand();
                    executor.executeCommand(cgc);
                    break;

                case CUC:
                    ConfigUpdaterCommand cuc = new ConfigUpdaterCommand();
                    executor.executeCommand(cuc);
                    break;

                case CWC:
                    ConfigWatcherCommand cwc = new ConfigWatcherCommand();
                    executor.executeCommand(cwc);
                    break;

                case DGC:
                    DeleteGroupCommand dgc = new DeleteGroupCommand();
                    executor.executeCommand(dgc);
                    break;

                case JGC:
                    JoinGroupCommand jgc = new JoinGroupCommand();
                    executor.executeCommand(jgc);
                    break;

                case LGC:
                    ListGroupCommand lgc = new ListGroupCommand();
                    executor.executeCommand(lgc);
                    break;


            }
        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }


    }
}
