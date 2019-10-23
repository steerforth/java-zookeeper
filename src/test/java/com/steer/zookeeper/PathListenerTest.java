package com.steer.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 监听节点的变化
 */
public class PathListenerTest {
    private Logger LOGGER = LoggerFactory.getLogger(CuratorTest.class);

    private CuratorFramework client;

    private static final String rootPath = "/dass";

    @Before
    public void initClient(){
        RetryPolicy policy=new ExponentialBackoffRetry(1000,3);
        client = CuratorFrameworkFactory.builder().connectString("192.168.2.204:2181")
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(3000)
                .retryPolicy(policy)
//                .namespace("steer")//独立操作的节点空间
                .build();
        client.start();
    }

    @After
    public void close(){
        client.close();
    }

    /**
     */
    @Test
    public void test(){
        TreeCache treeCache = new TreeCache(client, rootPath);

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event){
                ChildData eventData = event.getData();
                switch (event.getType()) {
                    case NODE_ADDED:
                        LOGGER.warn("{}节点添加{},添加数据为{}",rootPath,eventData.getPath(),new String(eventData.getData()));
                        break;
                    case NODE_UPDATED:
                        LOGGER.warn("{}节点数据更新,更新数据为：{},版本为{}",eventData.getPath(),new String(eventData.getData()), eventData.getStat().getVersion());
                        break;
                    case NODE_REMOVED:
                        LOGGER.warn("{}节点被删除,数据为:{}",eventData.getPath(),new String(eventData.getData()));
                        break;
                    case CONNECTION_SUSPENDED:
                        LOGGER.warn("{}节点挂起",eventData.getPath());
                        break;
                    case INITIALIZED:
                        LOGGER.warn("节点初始化");
                        break;
                    case CONNECTION_RECONNECTED:
                        LOGGER.warn("节点重连");
                        break;
                    case CONNECTION_LOST:
                        LOGGER.warn("节点连接丢失");
                        break;
                    default:
                        break;
                }
            }
        });

        try {
            treeCache.start();
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
