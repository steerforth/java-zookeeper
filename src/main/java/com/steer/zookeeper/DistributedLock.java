package com.steer.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 创建临时顺序节点
 * 根据争抢创建节点的顺序，依次执行任务
 * 执行完后删除对应节点
 *
 * 缺点：一个任务一把锁，一把锁一个zookeeper连接，最大连接数问题
 *
 * 待处理：
 * org.apache.zookeeper.KeeperException$ConnectionLossException: KeeperErrorCode = ConnectionLoss for /Locks
 */
public class DistributedLock {

    private static Logger LOGGER = LoggerFactory.getLogger(DistributedLock.class);
    private ZooKeeper zkClient;
    private static final String LOCK_ROOT_PATH = "/Locks";
    private static final String LOCK_NODE_NAME = "Lock_";
    private String lockPath;
    private PreNodeWatcher nodeWatcher = new PreNodeWatcher();

    private DistributedLock() {
    }

    public DistributedLock(String zkIp, int timeout) throws IOException {
        zkClient = new ZooKeeper(zkIp, timeout, (event)->{
            if (Watcher.Event.KeeperState.Disconnected == event.getState()){
                LOGGER.warn("{}失去连接",zkIp);
            }else if(Watcher.Event.KeeperState.SyncConnected == event.getState()){
                LOGGER.info("{}连接成功",zkIp);
            }
        });
    }

    public boolean acquireClock(){
        boolean createLock = createLock();
        if (createLock){
            return attemptLock();
        }
        return false;
    }

    public void unlock(){
        try {
            zkClient.delete(lockPath,-1);
            zkClient.close();
            LOGGER.info("释放锁:[{}]",lockPath);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    private boolean createLock() {
        try {
            Stat stat = zkClient.exists(LOCK_ROOT_PATH,false);
            if (stat == null){
                //可能会被其他线程创建
                zkClient.create(LOCK_ROOT_PATH,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e){
            if (LOCK_ROOT_PATH.equals(e.getPath())){
                LOGGER.warn("已存在节点：[{}]",e.getPath());
            }else{
                LOGGER.error(e.getMessage());
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            String lockPath = zkClient.create(LOCK_ROOT_PATH+"/"+LOCK_NODE_NAME,Thread.currentThread().getName().getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            this.lockPath = lockPath;
            LOGGER.info("成功创建锁节点[{}]",lockPath);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

    }

    private boolean attemptLock() {
        try {
           List<String> lockPaths = zkClient.getChildren(LOCK_ROOT_PATH,false);
           Collections.sort(lockPaths);

           int index = lockPaths.indexOf(lockPath.substring(LOCK_ROOT_PATH.length()+1));
//           LOGGER.info("lockPaths:{}",lockPaths);
//           LOGGER.info("{}:index:{}",lockPath,index);
           if (index == 0){
               LOGGER.info("成功获取到锁:[{}]",lockPath);
               return true;
           }else{
               String preLockPath = lockPaths.get(index-1);
               Thread.sleep(7000);
               Stat stat = zkClient.exists(LOCK_ROOT_PATH+"/"+preLockPath,nodeWatcher);
               if (stat == null){
                   //节点不存在，重新获取锁
                   attemptLock();
               }else{
                   LOGGER.info("等待前锁释放,preLockPath:{}",preLockPath);
                   synchronized (nodeWatcher){
                       nodeWatcher.wait();
                   }
                   attemptLock();
               }
           }
        } catch (KeeperException.ConnectionLossException e){
            LOGGER.error(e.getMessage());
            return false;
        }catch (KeeperException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }


    class PreNodeWatcher implements Watcher{
        @Override
        public void process(WatchedEvent event) {
            if (Event.EventType.NodeDeleted == event.getType()) {
                LOGGER.info("前锁[{}]已释放", event.getPath());
                synchronized (this) {
                    notifyAll();
                }
            }
        }
    }

}
