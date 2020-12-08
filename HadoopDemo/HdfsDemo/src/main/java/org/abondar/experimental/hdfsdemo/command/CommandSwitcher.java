package org.abondar.experimental.hdfsdemo.command;

import org.abondar.experimental.hadoopdemo.command.AbstractCommandSwitcher;
import org.abondar.experimental.hadoopdemo.command.HdfsCommands;


public class CommandSwitcher extends AbstractCommandSwitcher {
    @Override
    public void executeCommand(String[] args) {
        String cmd = getCommand(args);
        args = getArgs(args);

        try {

            switch (HdfsCommands.valueOf(cmd)) {



            }

        } catch (IllegalArgumentException ex) {
            System.out.println("Check documentation for command list");
            System.exit(1);
        }
    }
}
