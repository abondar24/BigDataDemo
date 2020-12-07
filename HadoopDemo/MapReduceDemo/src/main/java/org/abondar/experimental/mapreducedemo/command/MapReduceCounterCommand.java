package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.abondar.experimental.mapreducedemo.data.Temperature;
import org.abondar.experimental.mapreducedemo.parser.RecordParser;
import org.abondar.experimental.mapreducedemo.reducer.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MapReduceCounterCommand extends Configured implements Tool, Command {


    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final RecordParser parser = new RecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                int airTemperature = parser.getAirTemp();
                context.write(new Text(parser.getYear()),
                        new IntWritable(airTemperature));
            } else if (parser.isAirTempMalformed()){
                System.err.println("Ignoring possibly corrupt input: "+value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            } else if (!parser.isValidTemperature()){
                context.getCounter(Temperature.MISSING).increment(1);
            }

            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void execute(String[] args) {
       try {
           if (args.length!=2){
               System.err.println("Missing arguments");
               System.exit(2);
           }

           int exitCode = ToolRunner.run(new MapReduceCounterCommand(), args);
           System.exit(exitCode);
       } catch (Exception ex){
           System.err.println(ex.getMessage());
           System.exit(512);
       }
    }

}
