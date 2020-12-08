package org.abondar.experimental.hdfsdemo.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



import java.io.*;
import java.net.URI;

public class CopyFileCommand implements Command {

    @Override
    public void execute(String[] args) {
        try {
            String localSrc = args[0];
            String dst = args[1];

            InputStream is = new BufferedInputStream(new FileInputStream(localSrc));
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dst),conf);
            OutputStream os = fs.create(new Path(dst), () -> System.out.println("."));

            IOUtils.copyBytes(is,os,4096,true);
        } catch (IOException ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }
    }
}
