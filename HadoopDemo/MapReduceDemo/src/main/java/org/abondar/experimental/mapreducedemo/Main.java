package org.abondar.experimental.mapreducedemo;

import org.abondar.experimental.mapreducedemo.mapper.MaxTemperatureMapper;
import org.abondar.experimental.mapreducedemo.reducer.MaxTemperatureReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();


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

    }
}
