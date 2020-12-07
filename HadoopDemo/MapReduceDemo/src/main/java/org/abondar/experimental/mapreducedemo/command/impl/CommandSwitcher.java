package org.abondar.experimental.mapreducedemo.command.impl;

import org.abondar.experimental.mapreducedemo.command.BasicMapReduceCommand;

import java.util.Arrays;

public class CommandSwitcher {

    private final CommandExecutor executor;

    public CommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public void executeCommand(String[] args){
        String cmd = args[0].toUpperCase();
        args = Arrays.copyOfRange(args,1,args.length);

        try {

            switch (Commands.valueOf(cmd)){
                case BMR:
                    BasicMapReduceCommand bmr = new BasicMapReduceCommand();
                    executor.executeCommand(bmr,args);
                    break;
            }

        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }
    }
}
