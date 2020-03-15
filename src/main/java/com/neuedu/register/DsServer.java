package com.neuedu.register;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class DsServer {

    private static String connectString = "hadoop152:2181,hadoop153:2181,hadoop154:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    public void getConn() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, e->{});
    }

    /**
     * 注册服务器
     * @param hostname
     */
    public void registerServer(String hostname) throws KeeperException, InterruptedException {


        // 最关键的在于CreateMode.EPHEMERAL_SEQUENTIAL。因为我们创建的是临时序列节点，
        // 当server宕机后，和zookeeper的会话自然断开，导致临时序列节点被删除，这时候客户端就能监听到服务的变化，
        String path = zk.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"  active at "+ path);
    }

    public void business(String hostname) {
        System.out.println(hostname+" is working ....");
        while (true){

        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        DsServer dsServer = new DsServer();
        dsServer.getConn();
        dsServer.registerServer(args[0]);
        dsServer.business(args[0]);
    }
}
