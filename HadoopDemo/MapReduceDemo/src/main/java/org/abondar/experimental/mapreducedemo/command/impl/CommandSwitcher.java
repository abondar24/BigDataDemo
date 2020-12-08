package org.abondar.experimental.mapreducedemo.command.impl;

import org.abondar.experimental.mapreducedemo.command.ConfigurationPrinterCommand;
import org.abondar.experimental.mapreducedemo.command.JoinRecordCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceCombinerCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceCompressorCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceCounterCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceDriverCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceMinimalCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceSecondarySortCommand;
import org.abondar.experimental.mapreducedemo.command.MapReduceStationDistCacheFileCommand;
import org.abondar.experimental.mapreducedemo.command.MissingTemperatureFieldsCommand;
import org.abondar.experimental.mapreducedemo.command.PartitionByStationCommand;
import org.abondar.experimental.mapreducedemo.command.SequenceFileConverterCommand;
import org.abondar.experimental.mapreducedemo.command.SortDataPreprocessorCommand;
import org.abondar.experimental.mapreducedemo.command.SortTemperatureHashPartitionerCommand;
import org.abondar.experimental.mapreducedemo.command.SortTemperatureTotalOrderPartitionerCommand;

import java.util.Arrays;

public class CommandSwitcher {

    private final CommandExecutor executor;

    public CommandSwitcher(){
        this.executor = new CommandExecutor();
    }

    public void executeCommand(String[] args){
        String cmd = args[0].toUpperCase();
        args = Arrays.copyOfRange(args,1,args.length);

        try {

            switch (Commands.valueOf(cmd)){

                case CPC:
                    ConfigurationPrinterCommand cpc = new ConfigurationPrinterCommand();
                    executor.executeCommand(cpc,args);
                    break;

                case JRC:
                    JoinRecordCommand jrc = new JoinRecordCommand();
                    executor.executeCommand(jrc,args);
                    break;

                case MTFC:
                    MissingTemperatureFieldsCommand mtfc = new MissingTemperatureFieldsCommand();
                    executor.executeCommand(mtfc,args);
                    break;

                case MRC:
                    MapReduceCommand mrc = new MapReduceCommand();
                    executor.executeCommand(mrc,args);
                    break;

                case MRCC:
                    MapReduceCombinerCommand mrcc = new MapReduceCombinerCommand();
                    executor.executeCommand(mrcc,args);
                    break;

                case MRCN:
                    MapReduceCounterCommand mrcn = new MapReduceCounterCommand();
                    executor.executeCommand(mrcn,args);
                    break;

                case MRCO:
                    MapReduceCompressorCommand mrco = new MapReduceCompressorCommand();
                    executor.executeCommand(mrco,args);
                    break;

                case MRDC:
                    MapReduceDriverCommand mrdc = new MapReduceDriverCommand();
                    executor.executeCommand(mrdc,args);
                    break;

                case MRMC:
                    MapReduceMinimalCommand mrmc = new MapReduceMinimalCommand();
                    executor.executeCommand(mrmc,args);
                    break;

                case MRSD:
                    MapReduceStationDistCacheFileCommand mrsd = new MapReduceStationDistCacheFileCommand();
                    executor.executeCommand(mrsd,args);
                    break;

                case MRSS:
                    MapReduceSecondarySortCommand mrss = new MapReduceSecondarySortCommand();
                    executor.executeCommand(mrss,args);
                    break;

                case SDPC:
                    SortDataPreprocessorCommand sdpc = new SortDataPreprocessorCommand();
                    executor.executeCommand(sdpc,args);
                    break;

                case PSC:
                    PartitionByStationCommand psc = new PartitionByStationCommand();
                    executor.executeCommand(psc,args);
                    break;

                case SFCC:
                    SequenceFileConverterCommand sfcc = new SequenceFileConverterCommand();
                    executor.executeCommand(sfcc,args);
                    break;

                case STHP:
                    SortTemperatureHashPartitionerCommand stpc = new SortTemperatureHashPartitionerCommand();
                    executor.executeCommand(stpc,args);
                    break;

                case STTO:
                    SortTemperatureTotalOrderPartitionerCommand stto = new SortTemperatureTotalOrderPartitionerCommand();
                    executor.executeCommand(stto,args);
                    break;


            }

        } catch (IllegalArgumentException ex){
            System.out.println("Check documentation for command list");
            System.exit(1);
        }
    }
}
