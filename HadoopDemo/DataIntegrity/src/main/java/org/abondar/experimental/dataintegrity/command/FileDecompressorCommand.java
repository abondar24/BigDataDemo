package org.abondar.experimental.dataintegrity.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class FileDecompressorCommand implements Command {

    @Override
    public void execute(String[] args) {
        String uri = args[0];
        Configuration conf = new Configuration();


        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if (codec == null){
            System.err.println("No codec found for "+uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri,codec.getDefaultExtension());

        InputStream in = null;
        OutputStream out = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),conf);
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in,out,conf);
        }catch (IOException ex){
            System.err.println(ex.getMessage());
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
