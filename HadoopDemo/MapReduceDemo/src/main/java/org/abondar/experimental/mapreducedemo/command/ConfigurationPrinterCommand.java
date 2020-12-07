package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

public class ConfigurationPrinterCommand extends Configured implements Tool, Command {

    static {
        Configuration.addDefaultResource("hadoop-cluster.xml");
        Configuration.addDefaultResource("hadoop-local.xml");
        Configuration.addDefaultResource("hadoop-localhost.xml");
    }


    @Override
    public int run(String[] strings)  {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }



    @Override
    public void execute(String[] args) {
        try {
            if (args.length!=2){
                System.err.println("Missing arguments");
                System.exit(2);
            }

            int exitCode = ToolRunner.run(new ConfigurationPrinterCommand(), new String[]{"hadoop-cluster.xml"});
            System.exit(exitCode);
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }

    }
}
