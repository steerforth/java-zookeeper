package com.steer.zookeeper;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class ZkServerTest {
    private Logger log = LoggerFactory.getLogger(DistributedLockTest.class);
    @Test
    public void test() throws IOException, InterruptedException {
        InetSocketAddress addr = new InetSocketAddress("192.168.2.175", 2184);

        int numConnections = 5000;
        int tickTime = 2000;
        String dataDirectory = System.getProperty("java.io.tmpdir");
        File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        NIOServerCnxnFactory standaloneServerFactory = new NIOServerCnxnFactory();
        standaloneServerFactory.configure(addr, numConnections);
        standaloneServerFactory.startup(server); // start the server.
        log.info("start the ZKServerAlone!");
        System.in.read();
    }
}
