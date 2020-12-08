package org.abondar.experimental.hdfsdemo;

import org.abondar.experimental.hdfsdemo.command.CommandSwitcher;

public class Main {

    public static void main(String[] args) {
        CommandSwitcher cs = new CommandSwitcher();
        if (args.length==0){
            System.out.println("Missing arguments. Please check documentation for available arguments");
            System.exit(0);
        }

        cs.executeCommand(args);
    }
}
