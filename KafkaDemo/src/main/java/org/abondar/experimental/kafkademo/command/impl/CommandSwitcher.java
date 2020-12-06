package org.abondar.experimental.kafkademo.command.impl;


import org.abondar.experimental.kafkademo.command.MultiThreadedConsumerCommand;
import org.abondar.experimental.kafkademo.command.ConsumerCommand;
import org.abondar.experimental.kafkademo.command.CustomPartitioningProducerCommand;
import org.abondar.experimental.kafkademo.command.ProducerCommand;
import org.abondar.experimental.kafkademo.command.KTableCommand;
import org.abondar.experimental.kafkademo.command.ProcessorCommand;
import org.abondar.experimental.kafkademo.command.StreamJoinerCommand;
import org.abondar.experimental.kafkademo.command.StreamProcessorCommand;
import org.abondar.experimental.kafkademo.stream.StreamStoreCommand;

public class CommandSwitcher {

    private final CommandExecutor executor;

    public CommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public void executeCommand(String cmd){
        try {
            switch (Commands.valueOf(cmd)){

                case COC:
                    ConsumerCommand coc = new ConsumerCommand();
                    executor.executeCommand(coc);
                    break;

                case CPC:
                    CustomPartitioningProducerCommand cpc = new CustomPartitioningProducerCommand();
                    executor.executeCommand(cpc);
                    break;

                case KTC:
                    KTableCommand ktc = new KTableCommand();
                    executor.executeCommand(ktc);
                    break;

                case MTC:
                    MultiThreadedConsumerCommand mtc = new MultiThreadedConsumerCommand();
                    executor.executeCommand(mtc);
                    break;

                case POC:
                    ProcessorCommand poc = new ProcessorCommand();
                    executor.executeCommand(poc);
                    break;

                case PRC:
                    ProducerCommand prc = new ProducerCommand();
                    executor.executeCommand(prc);
                    break;

                case SJC:
                    StreamJoinerCommand sjc = new StreamJoinerCommand();
                    executor.executeCommand(sjc);
                    break;

                case SPC:
                    StreamProcessorCommand spc = new StreamProcessorCommand();
                    executor.executeCommand(spc);
                    break;

                case SSC:
                    StreamStoreCommand ssc = new StreamStoreCommand();
                    executor.executeCommand(ssc);
                    break;
            }
        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }


    }
}
