package org.abondar.experimental.hdfsdemo.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

public class ListStatusCommand implements Command {

    @Override
    public void execute(String[] args) {

        try {
            String uri = args[0];

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri),conf);

            Path[] paths = new Path[args.length];
            for (int i =0; i<paths.length;i++){
                paths[i] = new Path(args[i]);
            }

            FileStatus[] status = fs.listStatus(paths);
            Path[] listedPath = FileUtil.stat2Paths(status);
            for (Path p : listedPath){
                System.out.println(p);
            }
        } catch (IOException ex){
            System.err.println(ex.getMessage());
            System.exit(512);
        }

    }
}
