package org.abondar.experimental.dataintegrity.command;

import org.abondar.experimental.hadoopdemo.command.AbstractCommandSwitcher;

public class CommandSwitcher extends AbstractCommandSwitcher {
    @Override
    public void executeCommand(String[] args) {
        String cmd = getCommand(args);
        args = getArgs(args);

        try {

            switch (Commands.valueOf(cmd)) {
                case FDC:
                    FileDecompressorCommand fdc = new FileDecompressorCommand();
                    executor.executeCommand(fdc,args);
                    break;

                case SFWC:
                    SequenceFileWriteCommand sfwc = new SequenceFileWriteCommand();
                    executor.executeCommand(sfwc,args);
                    break;

                case SFRC:
                    SequenceFileReadCommand sfrc = new SequenceFileReadCommand();
                    executor.executeCommand(sfrc,args);
                    break;

                case TIC:
                    TextIteratorCommand tic = new TextIteratorCommand();
                    executor.executeCommand(tic,args);
                    break;

            }

        } catch (IllegalArgumentException ex) {
            System.out.println("Check documentation for command list");
            System.exit(1);
        }
    }
}
