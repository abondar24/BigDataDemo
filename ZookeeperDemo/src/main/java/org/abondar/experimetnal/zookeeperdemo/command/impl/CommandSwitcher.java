package org.abondar.experimetnal.zookeeperdemo.command.impl;

public class CommandSwitcher {

    private final CommandExecutor executor;

    public CommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public void executeCommand(String cmd){
        try {
            switch (Commands.valueOf(cmd)){

            }
        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }


    }
}
