package org.abondar.experimental.mapreducedemo.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.abondar.experimental.mapreducedemo.data.Temperature;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MissingTemperatureFieldsCommand extends Configured implements Tool, Command {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: mtfc job_id");
            ToolRunner.printGenericCommandUsage(System.err);
            return 2;
        }
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobID));
        if (job == null) {
            System.err.printf("No job with ID %s found.\n", jobID);
            return 3;
        }
        if (!job.isComplete()) {
            System.err.printf("Job %s is not complete\n", jobID);
            return 4;
        }

        Counters counters = job.getCounters();
        long missing = counters.findCounter(Temperature.MISSING).getValue();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

        System.out.printf("Records with missing temperature fields: %.2f%%\n",
                100.0 * missing / total);

        return 0;
    }


    @Override
    public void execute(String[] args) {
         try {
             int exitCode = ToolRunner.run(new MissingTemperatureFieldsCommand(), args);
             System.exit(exitCode);
         } catch (Exception ex){
             System.err.println(ex.getMessage());
             System.exit(512);
         }
    }
}
