package com.steer.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * org.apache.zookeeper的版本需大于3.4.14，之前的有中等程度的安全问题
 */
public class ZookeeperTest {
    private static Logger LOGGER = LoggerFactory.getLogger(ZookeeperTest.class);
    private static final int TIME_OUT = 2000;
    private static final String HOST = "127.0.0.1";
    private ZooKeeper zooKeeper;

    @Before
    public void initZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(HOST, TIME_OUT, new SteerWatcher());
    }

    @After
    public void closeZookeeper() throws InterruptedException {
        this.zooKeeper.close();
    }

    @Test
    public void testCreateNode() throws InterruptedException, KeeperException {
        String path = "/test2";
        String value = "test123";
        LOGGER.info("创建节点:{}",path);
        //权限为公开权限
        String res = zooKeeper.create(path,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("创建节点:{},数据:{}成功",res,value);
        Thread.sleep(3000);
    }

    @Test
    public void testGetNode() throws KeeperException, InterruptedException {
        String path = "/test2";
        LOGGER.info("获取节点:{}的信息",path);
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(path,true,stat);
        LOGGER.info("获取节点:[{}]数据成功:[{}],version:{}",path,new String(data),stat.getVersion());
        Thread.sleep(10000);
    }

    @Test
    public void testExist() throws KeeperException, InterruptedException {
        String path = "/test4";
        Stat stat = zooKeeper.exists(path,true);
        if (stat == null){
            LOGGER.info("节点{}不存在",path);
        }
        LOGGER.info("节点{}存在,version:{}",path,stat.getVersion());
        Thread.sleep(3000);
    }

    @Test
    public void testUpdateNode() throws InterruptedException, KeeperException {
        String path = "/test";
        String newValue = "test456";
        int ver = 1;
        LOGGER.info("修改节点:{}",path);
        //更新成功后,stat的version数据会递增1
        Stat stat = zooKeeper.setData(path, newValue.getBytes(), ver);
        LOGGER.info("设置节点{}数据为{}成功!新的version为:{}",path,newValue,stat.getVersion());
        Thread.sleep(3000);
    }

    @Test
    public void testDeleteNode() throws KeeperException, InterruptedException {
        String path = "/test2";
        int ver = 0;
        LOGGER.info("删除节点:{}",path);
        zooKeeper.delete(path,ver);
        LOGGER.info("删除节点{},版本:{}成功",path,ver);
        Thread.sleep(3000);
    }

    @Test
    public void testChildren() throws KeeperException, InterruptedException {
        String path = "/test";
        Stat stat = new Stat();
        List<String> childrenPath = zooKeeper.getChildren(path,true,stat);
        LOGGER.info("获取节点{},版本:{}下子节点成功",path,stat.getVersion());
        childrenPath.stream().forEach(p->LOGGER.info(p));
        Thread.sleep(3000);
    }

    /**
     * 节点权限控制
     * @throws NoSuchAlgorithmException
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testAcl() throws NoSuchAlgorithmException, KeeperException, InterruptedException {
        ArrayList<ACL> acls = new ArrayList<>();
        ACL aclIp = new ACL(ZooDefs.Perms.READ|ZooDefs.Perms.WRITE,new Id("ip","192.168.2.171"));
        String authStr = DigestAuthenticationProvider.generateDigest("myUsername:123456");
        ACL aclDigest = new ACL(ZooDefs.Perms.READ|ZooDefs.Perms.WRITE,new Id("digest",authStr));
        acls.add(aclIp);
        acls.add(aclDigest);

        String path = zooKeeper.create("/test3","123".getBytes(),acls,CreateMode.PERSISTENT);
        LOGGER.info("创建带权限节点{}成功",path);
        Thread.sleep(3000);
    }


}
