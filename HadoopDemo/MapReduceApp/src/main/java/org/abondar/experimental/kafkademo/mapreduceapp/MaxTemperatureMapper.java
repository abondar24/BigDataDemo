package org.abondar.experimental.kafkademo.mapreduceapp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private RecordParser parser = new RecordParser();

    enum Temperature {
        OVER_100,
        MALFORMED
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()){
            int airTemp = parser.getAirTemp();
            if (airTemp>1000){
                System.err.println("Temperature over 100 degres for input: "+value);
                context.setStatus("Detected possibly corrupt record: see logs");
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            context.write(new Text(parser.getYear()), new IntWritable(airTemp));
        } else if (parser.isAirTempMalformed()) {
            System.err.println("Ignoring possibly corrupt input: "+value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        }
    }

}
