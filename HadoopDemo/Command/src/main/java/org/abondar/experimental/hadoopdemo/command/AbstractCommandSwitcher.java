package org.abondar.experimental.hadoopdemo.command;


import java.util.Arrays;

public abstract class AbstractCommandSwitcher {

    protected final CommandExecutor executor;

    public AbstractCommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public abstract void executeCommand(String[] args);

    protected String getCommand(String[] args){
        return args[0].toUpperCase();
    }

    protected String[] getArgs(String[] args){
        return Arrays.copyOfRange(args,1,args.length);
    }
}
