package org.abondar.experimetnal.zookeeperdemo;

public class CreateGroup  {


    public static void main(String[] args) throws Exception {
        ConnectionWatcher watcher = new ConnectionWatcher();
        watcher.connect(args[0]);
        watcher.create(args[1]);
        watcher.close();
    }
}
