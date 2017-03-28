package org.abondar.experimental.hdfsdemo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


public class FileSystemCat {
    public static void main(String[] args) {
        String uri = args[0];
        Configuration configuration = new Configuration();
        InputStream is = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(uri),configuration);
            is = fs.open(new Path(uri));
            IOUtils.copyBytes(is,System.out,4096,false);
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } finally {
            IOUtils.closeStream(is);
        }
    }
}
