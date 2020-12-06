package org.abondar.experimental.kafkademo;

import org.abondar.experimental.kafkademo.command.impl.CommandSwitcher;

public class Main {
    public static void main(String[] args) {
        CommandSwitcher cs = new CommandSwitcher();
        if (args.length==0){
            System.out.println("Missing argument. Please check documentation for available arguments");
            System.exit(0);
        }
        String cmd = args[0].toUpperCase();
        cs.executeCommand(cmd);
    }
}
