package org.abondar.experimental.hdfsdemo.command;

import org.abondar.experimental.hadoopdemo.command.AbstractCommandSwitcher;


public class CommandSwitcher extends AbstractCommandSwitcher {
    @Override
    public void executeCommand(String[] args) {
        String cmd = getCommand(args);
        args = getArgs(args);

        try {

            switch (HdfsCommands.valueOf(cmd)) {
                case CFC:
                    CopyFileCommand cfc =  new CopyFileCommand();
                    executor.executeCommand(cfc,args);
                    break;


            }

        } catch (IllegalArgumentException ex) {
            System.out.println("Check documentation for command list");
            System.exit(1);
        }
    }
}
