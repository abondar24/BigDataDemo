package org.abondar.experimental.mapreducedemo.mapper;


import org.abondar.experimental.mapreducedemo.parser.RecordParser;
import org.abondar.experimental.mapreducedemo.data.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private RecordParser parser = new RecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        context.write(new TextPair(parser.getStationId(),"1"),value);
    }
}
