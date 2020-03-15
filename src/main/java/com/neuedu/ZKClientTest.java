package com.neuedu;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZKClientTest {

    private ZooKeeper zkClient;
    private static final String ZK_CONN="hadoop152:2181,hadoop153:2181,hadoop154:2181";


    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(ZK_CONN,2000,e->{
            System.out.println("default watcher:"+e);

        });

    }

    @After
    public void destroy() throws InterruptedException {
        zkClient.close();
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", e-> System.out.println("ls watcher:"+e));
        for(String child:children){
            System.out.println("child:"+child);
        }
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        String path = zkClient.create("/idea", "haha".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new Stat());
        System.out.println(path);
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData("/idea", "哈哈哈哈".getBytes(StandardCharsets.UTF_8), 1);
        System.out.println(stat.getDataLength());
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/idea", e -> {
            System.out.println("get watcher:"+e);
        }, new Stat());
        System.out.println("data:"+new String(data));
        while(true){

        }
    }

    @Test
    public void stat() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/idea", false);
        System.out.println("version"+stat.getVersion());
        System.out.println("num children"+stat.getNumChildren());
        System.out.println("length"+stat.getDataLength());
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/idea", false);
        if(stat!=null)
            zkClient.delete("/idea",stat.getVersion());
    }

    @Test
    public void loopRegisterTest() throws KeeperException, InterruptedException {
        loopRegister();

        while (true) {

        }
    }

    public void loopRegister() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/idea", e -> {
            try {
                loopRegister();
            } catch (KeeperException | InterruptedException ex) {
                ex.printStackTrace();
            }

        }, new Stat());
        System.out.println(new String(data));

    }


}
