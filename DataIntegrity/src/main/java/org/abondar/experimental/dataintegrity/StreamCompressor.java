package org.abondar.experimental.dataintegrity;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {
    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf =new Configuration();
        CompressionCodec code= (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);
        CompressionOutputStream out = code.createOutputStream(System.out);
        IOUtils.copyBytes(System.in,out,4096,false);
        out.finish();
    }
}