package com.steer.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SteerWatcher implements Watcher {
    private static Logger LOGGER = LoggerFactory.getLogger(SteerWatcher.class);
    @Override
    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected.equals(event.getState())){
            if(event.getType() == Event.EventType.None && null == event.getPath()) {
                // 最初与zk服务器建立好连接
                LOGGER.info("连接事件!");
            } else if(Event.EventType.NodeDataChanged.equals(event.getType())) {
                LOGGER.info("节点数据变化事件");
            } else if(Event.EventType.NodeCreated.equals(event.getType())){
                LOGGER.info("节点创建事件");
            } else if(Event.EventType.NodeDeleted.equals(event.getType())){
                LOGGER.info("节点删除事件");
            } else if(Event.EventType.NodeChildrenChanged.equals(event.getType())){
                LOGGER.info("子节点变化事件");
            }
        }else if(Event.KeeperState.Disconnected.equals(event.getState())){
            LOGGER.info("连接断开!");
        }else if(Event.KeeperState.AuthFailed.equals(event.getState())){
            LOGGER.info("授权失败!");
        }else if(Event.KeeperState.ConnectedReadOnly.equals(event.getState())){
            LOGGER.info("连接只读!");
        }
    }
}
