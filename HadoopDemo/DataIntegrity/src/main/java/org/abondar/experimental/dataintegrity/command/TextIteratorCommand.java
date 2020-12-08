package org.abondar.experimental.dataintegrity.command;


import org.abondar.experimental.hadoopdemo.command.Command;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

public class TextIteratorCommand implements Command {

    @Override
    public void execute(String[] args) {
        Text t = new Text("\u0041\u00DF\uD801\uDC00");
        ByteBuffer buffer = ByteBuffer.wrap(t.getBytes(),0,t.getLength());
        int cp;

        while (buffer.hasRemaining() && (cp = Text.bytesToCodePoint(buffer))!= -1){
            System.out.println(Integer.toHexString(cp));
        }
    }
}
