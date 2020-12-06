package org.abondar.experimetnal.zookeeperdemo.command;


import org.abondar.experimetnal.zookeeperdemo.ActiveKeyValueStore;
import org.abondar.experimetnal.zookeeperdemo.command.impl.Command;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ConfigWatcherCommand implements Watcher, Command {

    private ActiveKeyValueStore store;

    public void displayConfig() throws Exception {
        String value = store.read(CommandUtil.CONFIG_PATH, this);
        System.out.printf("Read %s as %s\n",CommandUtil.CONFIG_PATH, value);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
          if (watchedEvent.getType() == Event.EventType.NodeDataChanged){
              try {
                  displayConfig();
              } catch (Exception ex){
                  System.err.println(ex.getMessage());
              }
          }
    }


    @Override
    public void execute() {
        try {
            store  = new ActiveKeyValueStore();
            store.connect(CommandUtil.ZOOKEEPER_HOST);

            displayConfig();

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception ex){
            System.err.println(ex.getMessage());
            System.exit(1);
        }

    }
}
