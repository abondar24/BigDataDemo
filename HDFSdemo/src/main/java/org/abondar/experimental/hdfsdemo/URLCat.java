package org.abondar.experimental.hdfsdemo;


import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class URLCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {
        InputStream is = null;
        try {
            is = new URL(args[0]).openStream();
            IOUtils.copyBytes(is,System.out,4096,false);
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } finally {
            IOUtils.closeStream(is);
        }
    }
}
