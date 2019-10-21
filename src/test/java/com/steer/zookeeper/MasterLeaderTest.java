package com.steer.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MasterLeaderTest {
    private Logger LOGGER = LoggerFactory.getLogger(MasterLeaderTest.class);
    //设置客户端的数量
    static int countClient=10;

    //设置leader的路径
    static String select_path="/selector";

    public CuratorFramework initClient(){
        RetryPolicy policy=new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(3000)
                .retryPolicy(policy)
                .namespace("steer")//独立操作的节点空间
                .build();
        return client;
    }


    @Test
    public void testMasterLeader() throws IOException {
        final List<LeaderLatch> leaders=new ArrayList<>();
        final List<CuratorFramework> clients=new ArrayList<>();

        for(int i=0;i<countClient;i++) {
            //建立模拟客户端
            CuratorFramework client = this.initClient();
            clients.add(client);
            //Leader选举   注意每个客户端的路径都要相同才会在同一个组中
            final LeaderLatch leader = new LeaderLatch(client, select_path, "clent" + i);
            leader.addListener(new LeaderLatchListener() {
                //获取leader权限时执行
                public void isLeader() {
                    LOGGER.info("I am Leader:[{}]",leader.getId());
                    //拥有leader权限的长度
                    try {
                        final int waitSeconds = (int) (5 * Math.random()) + 1;
                        Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));

                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }

                    //模拟让出leader 权限
                    if(leader !=null) {
                        try {
                            //关闭是通知所有客户端
                            leader.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void notLeader() {
                    LOGGER.info("I am not Leader:[{}]",leader.getId());
                }
            });

            leaders.add(leader);
            //客户端启动
            client.start();

            try {
                //必须启动LeaderLatch: leaderLatch.start(); 一旦启动， LeaderLatch会和其它使用相同latch path的其它LeaderLatch交涉，然后随机的选择其中一个作为leader
                leader.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        System.in.read();

        for(CuratorFramework client:clients){
            CloseableUtils.closeQuietly(client);
        }

        for(LeaderLatch leader:leaders){
            CloseableUtils.closeQuietly(leader);
        }


    }


}
