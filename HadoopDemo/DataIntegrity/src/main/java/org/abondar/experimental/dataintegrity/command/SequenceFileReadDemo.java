package org.abondar.experimental.dataintegrity.command;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceFileReadDemo {
    public static void main(String[] args) {
        String uri = args[0];
        Configuration conf = new Configuration();
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option fileOpt = SequenceFile.Reader.file(path);


        try {
            reader = new SequenceFile.Reader(conf,fileOpt);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
            long pos = reader.getPosition();

            while (reader.next(key,value)){
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n",pos,syncSeen,key,value);
            }
        }catch (IOException e){
            System.err.println(e.getMessage());
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
