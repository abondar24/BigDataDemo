package org.abondar.experimetnal.zookeeperdemo.command.impl;

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
                case CG:
                    CreateGroupCommand cg = new CreateGroupCommand();
                    executor.executeCommand(cg);
                    break;

                case JG:
                    JoinGroupCommand jg = new JoinGroupCommand();
                    executor.executeCommand(jg);
                    break;

                case LG:
                    ListGroupCommand lg = new ListGroupCommand();
                    executor.executeCommand(lg);
                    break;


            }
        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }


    }
}
