package org.abondar.experimental.mapreducedemo.command.impl;

public class CommandExecutor {

    public void executeCommand(Command command,String[] args){
        command.execute(args);
    }
}
