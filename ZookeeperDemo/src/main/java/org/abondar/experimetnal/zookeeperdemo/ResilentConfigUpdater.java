package org.abondar.experimetnal.zookeeperdemo;

import org.apache.zookeeper.KeeperException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by abondar on 4/5/17.
 */
public class ResilentConfigUpdater {

    public static final String PATH = "/config";
    private ActiveKeyValueStore store;
    private Random random = new Random();


    public ResilentConfigUpdater(String hosts) throws Exception {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void run() throws Exception {
        while (true){
            String value = random.nextInt(100) + "";
            store.write(PATH, value);
            System.out.printf("Set %s to %s\n",PATH,value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    public static void main(String[] args) throws Exception {
        while (true){
            try {
                ResilentConfigUpdater configUpdater = new ResilentConfigUpdater(args[0]);
                configUpdater.run();
            } catch (KeeperException.SessionExpiredException ex){
                System.err.println(ex.getMessage());
            } catch (KeeperException ex){
                System.err.println(ex.getMessage());
                break;
            }
        }
    }
}
