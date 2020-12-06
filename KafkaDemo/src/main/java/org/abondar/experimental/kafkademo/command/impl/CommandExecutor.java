package org.abondar.experimental.kafkademo.command.impl;

public class CommandExecutor {

    public void executeCommand(Command command){
        command.execute();
    }
}
