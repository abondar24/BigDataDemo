package org.abondar.experimental.mapreducedemo.mapper;


import org.abondar.experimental.mapreducedemo.parser.StationMetadataParser;
import org.abondar.experimental.mapreducedemo.data.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair,Text > {

    private StationMetadataParser parser = new StationMetadataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (parser.parse(value)){
            context.write(new TextPair(parser.getStationId(),"0"),
                    new Text(parser.getStationName()));
        }
    }
}
