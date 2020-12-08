package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.abondar.experimental.mapreducedemo.data.IntPair;
import org.abondar.experimental.mapreducedemo.parser.RecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MapReduceSecondarySortCommand extends Configured implements Tool, Command {

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {

        RecordParser parser = new RecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new IntPair(Integer.parseInt(parser.getYear()), parser.getAirTemp()), NullWritable.get());
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {

        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {

        @Override
        public int getPartition(IntPair key, NullWritable nullWritable, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair pair1 = (IntPair) a;
            IntPair pair2 = (IntPair) b;

            int cmp = IntPair.compare(pair1.getFirst(), pair2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return -IntPair.compare(pair1.getSecond(), pair2.getSecond());
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair pair1 = (IntPair) a;
            IntPair pair2 = (IntPair) b;

            return IntPair.compare(pair1.getFirst(), pair2.getFirst());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    @Override
    public void execute(String[] args) {
        try {
            int exitCode = ToolRunner.run(new MapReduceSecondarySortCommand(), args);
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
