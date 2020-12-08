package org.abondar.experimental.dataintegrity.command;



import org.abondar.experimental.hadoopdemo.command.Command;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class StreamCompressorCommand implements Command {

    @Override
    public void execute(String[] args) {

        try {
            String codecClassname = args[0];
            Class<?> codecClass = Class.forName(codecClassname);
            Configuration conf =new Configuration();
            CompressionCodec code= (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);
            CompressionOutputStream out = code.createOutputStream(System.out);
            IOUtils.copyBytes(System.in,out,4096,false);
            out.finish();
        } catch (IOException | ClassNotFoundException ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }

    }
}
