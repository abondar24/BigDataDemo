package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.mapreducedemo.command.impl.Command;
import org.abondar.experimental.mapreducedemo.data.TextPair;
import org.abondar.experimental.mapreducedemo.mapper.JoinRecordMapper;
import org.abondar.experimental.mapreducedemo.mapper.JoinStationMapper;
import org.abondar.experimental.mapreducedemo.reducer.JoinReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinRecordCommand extends Configured implements Tool, Command {


    public static class KeyPartitioner extends Partitioner<TextPair, Text>{

        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length!=3) {
            System.err.println("Usage: jrc <input><station input><output>");
            return -1;
        }

        Job job = Job.getInstance(getConf(),"Join weather records with station names");
        job.setJarByClass(getClass());
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job,inputPath, TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job,stationInputPath,TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void execute(String[] args) {
        try {
            int exitCode = ToolRunner.run(new JoinRecordCommand(), args);
            if (args.length != 3) {
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
