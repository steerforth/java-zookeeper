package com.steer.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * curator 版本和zookeeper的版本有对应关系
 */
public class CuratorTest {
    private Logger LOGGER = LoggerFactory.getLogger(CuratorTest.class);

    private CuratorFramework client;

    @Before
    public void initClient(){
        RetryPolicy policy=new ExponentialBackoffRetry(1000,3);
        client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(3000)
                .retryPolicy(policy)
                .namespace("steer")//独立操作的节点空间
                .build();
        client.start();
    }

    @After
    public void close(){
        client.close();
    }

    /**
     * 创建一个 允许所有人访问的 持久节点
     */
    @Test
    public void testCreate() throws Exception {
        if (client.isZk34CompatibilityMode()){
            LOGGER.info("当前在zookeeper3.4.X兼容模式运行");
        }
        client.create()
        .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
        .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
        .forPath("/test/child_01","123456".getBytes());
    }

    /**
     * 获取节点 /test/child_01 数据 和stat信息
     */
    @Test
    public void testGet() throws Exception {
        Stat node10Stat = new Stat();
        byte[] node10 = client.getData()
                .storingStatIn(node10Stat)//获取stat信息存储到stat对象
                .forPath("/test/child_01");
        LOGGER.info("该节点信息为:{}",new String(node10));
        LOGGER.info("该节点的数据版本号为:{}",node10Stat.getVersion());
    }

    /**
     * 获取节点信息 并且留下 Watcher事件 该Watcher只能触发一次
     *
     */
    @Test
    public void testWatchAndSet() throws Exception {
        byte[] bytes = client.getData()
                .usingWatcher(new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        LOGGER.info("=====>wathcer触发了!!");
                        LOGGER.info("=={}",event);
                    }
                })
                .forPath("/test/child_01");
        LOGGER.info("=====>获取到的节点数据为：{}",new String(bytes));
        //设置节点
        Stat stat = client.setData()
                .withVersion(-1)
                .forPath("/test/child_01", "I love you".getBytes());
        LOGGER.info("=====>修改之后的版本为：{}" , stat.getVersion());
        System.in.read();
    }


    /**
     * 删除node节点 不递归  如果有子节点,将报异常
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        Void aVoid = client.delete()
                .forPath("/test");
        LOGGER.info("=====>{}", aVoid);
    }

    /**
     * 递归删除
     * @throws Exception
     */
    @Test
    public void testDeleteAnyway() throws Exception {
        Void aVoid =  client.delete()
                .deletingChildrenIfNeeded()
                .forPath("/test");
        LOGGER.info("=====>{}", aVoid);
    }

    @Test
    public void checkNode() throws Exception {
        Stat existsNodeStat = client.checkExists().forPath("/test");
        if(existsNodeStat == null){
            LOGGER.info("=====>节点不存在");
            return;
        }
        if(existsNodeStat.getEphemeralOwner() > 0){
            LOGGER.info("=====>临时节点");
        }else{
            LOGGER.info("=====>持久节点");
        }
    }

    /**
     * 事务
     * @throws Exception
     */
    @Test
    public void testTransaction() throws Exception {
        client.inTransaction().check().forPath("/test")
                .and()
                .create().withMode(CreateMode.PERSISTENT).forPath("/test/child_02","data".getBytes())
                .and()
                .setData().withVersion(-1).forPath("/test/child_02","data2".getBytes())
                .and()
                .commit();
    }


    /**
     * eventType:
     * CREATE #create()
     * DELETE #delete()
     * EXISTS #checkExists()
     * GET_DATA #getData()
     * SET_DATA #setData()
     * CHILDREN #getChildren()
     * SYNC #sync(String,Object)
     * GET_ACL #getACL()
     * SET_ACL #setACL()
     * WATCHED #Watcher(Watcher)
     * CLOSING #close()作者：zhrowable
     *
     *
     * resultCode:
     *  0	OK，即调用成功
     * -4	ConnectionLoss，即客户端与服务端断开连接
     * -110	NodeExists，即节点已经存在
     * -112	SessionExpired，即会话过期
     * @throws Exception
     */
    @Test
    public void testAsyncCreate() throws Exception {
        Executor executor = Executors.newFixedThreadPool(2);
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .inBackground((curatorFramework, curatorEvent) -> {
                    LOGGER.info("eventType:{},resultCode:{}",curatorEvent.getType(),curatorEvent.getResultCode());
                },executor)
                .forPath("/test/child_03");
        System.in.read();
    }

}
