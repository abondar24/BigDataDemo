package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.abondar.experimental.mapreducedemo.parser.RecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PartitionByStationCommand extends Configured implements Tool, Command {


    static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final RecordParser parser = new RecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new Text(parser.getStationId()), value);
        }
    }

    static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> outputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                outputs.write(NullWritable.get(), value, key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MultipleOutputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void execute(String[] args) {
        try {
            int exitCode = ToolRunner.run(new PartitionByStationCommand(),args);
            if (args.length != 2) {
                System.err.println("Missing arguments");
                System.exit(2);
            }

            System.exit(exitCode);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            System.exit(512);
        }
    }


}
