package org.abondar.experimental.dataintegrity.command;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SequenceFileWriteDemo {

    private static final String[] DATA = {
            "I like burgers",
            "I like salo",
            "I like Borscht"
    };

    public static void main(String[] args) {
        String uri = args[0];
        Configuration conf = new Configuration();
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;

        SequenceFile.Writer.Option pathOpt =  SequenceFile.Writer.file(path);
        SequenceFile.Writer.Option keyOpt =  SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option valOpt =  SequenceFile.Writer.valueClass(value.getClass());

        try {
            writer = SequenceFile.createWriter(conf,pathOpt,keyOpt,valOpt);

            for (int i=0; i<100;i++){
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n",writer.getLength(),key,value);
                writer.append(key,value);
            }

        }catch (IOException ex){
            System.err.printf(ex.getMessage());
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
