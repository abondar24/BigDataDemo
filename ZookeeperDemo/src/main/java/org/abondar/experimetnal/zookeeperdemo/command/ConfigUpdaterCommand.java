package org.abondar.experimetnal.zookeeperdemo.command;


import org.abondar.experimetnal.zookeeperdemo.ActiveKeyValueStore;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConfigUpdaterCommand implements Command {


    @Override
    public void execute() {
        try {
            ActiveKeyValueStore store = new ActiveKeyValueStore();
            store.connect(CommandUtil.ZOOKEEPER_HOST);

            Random random = new Random();

            while (true){
                String value = random.nextInt(100) + "";
                store.write(CommandUtil.CONFIG_PATH, value);
                System.out.printf("Set %s to %s\n",CommandUtil.CONFIG_PATH,value);
                TimeUnit.SECONDS.sleep(random.nextInt(10));
            }
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(1);
        }
    }
}
