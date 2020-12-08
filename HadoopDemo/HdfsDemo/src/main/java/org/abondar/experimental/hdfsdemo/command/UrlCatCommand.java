package org.abondar.experimental.hdfsdemo.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class UrlCatCommand implements Command {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }


    @Override
    public void execute(String[] args) {
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
