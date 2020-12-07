package org.abondar.experimental.mapreducedemo.command;

import org.abondar.experimental.mapreducedemo.Main;
import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.abondar.experimental.mapreducedemo.mapper.MaxTemperatureMapper;
import org.abondar.experimental.mapreducedemo.reducer.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapReduceCommand implements Command {
    @Override
    public void execute(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: mrc <input path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();


        try {
            Job job = Job.getInstance(conf,"Max temperature");
            job.setJarByClass(Main.class);
            job.setJobName("Max temperature");
            job.setMapperClass(MaxTemperatureMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setReducerClass(MaxTemperatureReducer.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));


            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }
    }
}
