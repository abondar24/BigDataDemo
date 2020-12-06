package org.abondar.experimental.kafkademo.command.impl;


import org.abondar.experimental.kafkademo.command.ProducerCommand;

public class CommandSwitcher {

    private final CommandExecutor executor;

    public CommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public void executeCommand(String cmd){
        try {
            switch (Commands.valueOf(cmd)){
                case PRC:
                    ProducerCommand prc = new ProducerCommand();
                    executor.executeCommand(prc);
                    break;
            }
        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }


    }
}
