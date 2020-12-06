package org.abondar.experimetnal.zookeeperdemo.command.impl;

import org.abondar.experimetnal.zookeeperdemo.command.CreateGroupCommand;

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
            }
        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }


    }
}
