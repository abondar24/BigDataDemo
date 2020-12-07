package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceMinimalCommand extends Configured implements Tool, Command {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: mrmc <input> <output>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    @Override
    public void execute(String[] args) {
        try {
            int exitCode = ToolRunner.run(new MapReduceMinimalCommand(), args);
            System.exit(exitCode);
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }
    }
}
