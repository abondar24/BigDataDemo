package org.abondar.experimetnal.zookeeperdemo;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ConfigWatcher implements Watcher {

    private ActiveKeyValueStore store;

    public ConfigWatcher(String hosts) throws Exception {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void displayConfig() throws Exception {
        String value = store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n",ConfigUpdater.PATH, value);
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

    public static void main(String[] args) throws Exception {
        ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
        configWatcher.displayConfig();

        Thread.sleep(Long.MAX_VALUE);
    }
}
