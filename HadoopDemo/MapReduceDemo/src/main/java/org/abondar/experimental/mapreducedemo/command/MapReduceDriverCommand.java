package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.abondar.experimental.mapreducedemo.mapper.MaxTemperatureMapper;
import org.abondar.experimental.mapreducedemo.reducer.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceDriverCommand extends Configured implements Tool, Command {
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: mrd <input <output>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }


        Job job = Job.getInstance(getConf(), "MaxTemperature");
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(getClass());


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true) ? 1 : 0;
    }


    @Override
    public void execute(String[] args) {
        try {
            int exitCode = ToolRunner.run(new MapReduceDriverCommand(),args);
            System.exit(exitCode);
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }

    }
}
