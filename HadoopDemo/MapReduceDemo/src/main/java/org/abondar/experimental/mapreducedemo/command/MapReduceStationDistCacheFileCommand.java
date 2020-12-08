package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.abondar.experimental.mapreducedemo.data.StationMetadata;
import org.abondar.experimental.mapreducedemo.parser.RecordParser;
import org.abondar.experimental.mapreducedemo.reducer.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class MapReduceStationDistCacheFileCommand extends Configured implements Tool, Command {

    static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private RecordParser parser = new RecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemp()));
            }
        }
    }

    static class MaxTemperatureReducerWithStationLookup extends Reducer<Text, IntWritable, Text, IntWritable> {
        private StationMetadata metadata;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            metadata = new StationMetadata();
            metadata.initialize(new File("stations.txt"));
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String stationName = metadata.getStationName(key.toString());

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(new Text(stationName), new IntWritable(maxValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(StationTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    @Override
    public void execute(String[] args) {
        try {
            int exitCode = ToolRunner.run(new MapReduceStationDistCacheFileCommand(), args);
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
