package com.steer.zookeeper;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DistributedLockTest {
    private Logger LOGGER = LoggerFactory.getLogger(DistributedLockTest.class);

    /**
     * 默认zookeeper给每个客户端IP使用的连接数为10个
     * maxClientCnxns
     * @throws IOException
     */
    @Test
    public void test() {

            for (int i = 0; i < 20; i++) {

                new Thread(()->{
                    DistributedLock lock1 = null;
                    try {
                        lock1 = new DistributedLock("127.0.0.1",2000);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (lock1.acquireClock()){
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }else{
                        LOGGER.warn("未获取到锁，执行任务失败！！！");
                    }
                    lock1.unlock();

                }).start();
            }

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
