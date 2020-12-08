package org.abondar.experimental.dataintegrity.command;

import org.abondar.experimental.hadoopdemo.command.AbstractCommandSwitcher;

public class CommandSwitcher extends AbstractCommandSwitcher {
    @Override
    public void executeCommand(String[] args) {
        String cmd = getCommand(args);
        args = getArgs(args);

        try {

            switch (Commands.valueOf(cmd)) {



            }

        } catch (IllegalArgumentException ex) {
            System.out.println("Check documentation for command list");
            System.exit(1);
        }
    }
}
