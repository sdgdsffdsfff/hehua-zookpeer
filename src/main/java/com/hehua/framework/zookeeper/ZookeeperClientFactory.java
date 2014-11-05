package com.hehua.framework.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.helpers.LogLog;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClientFactory {

    private ZookeeperClientFactory() {
    }

    private final static ZookeeperClientFactory INSTANCE = new ZookeeperClientFactory();

    public static ZookeeperClientFactory getInstance() {
        return INSTANCE;
    }

    public ZooKeeper createClient(Watcher watcher) throws IOException {
        InetAddress[] addresses = null;
        List<String> hp = new ArrayList<String>();
        try {
            if (addresses == null) {
                addresses = InetAddress.getAllByName(ZookeeperConstants.zkDOMAIN);
            }
            for (InetAddress ia : addresses) {
                hp.add(ia.getHostAddress() + ":" + ZookeeperConstants.zkPORT);
            }
        } catch (UnknownHostException e) {
            LogLog.error("Cannt find zookeeper cluster", e);
            throw new RuntimeException(e);
        }

        String join = StringUtils.join(hp, ",");
        LogLog.debug("Try to connect to ZooKeeper:\t" + join);
        ZooKeeper zk = new ZooKeeper(join, ZookeeperConstants.zkSESSION_TIMEOUT, watcher);
        return zk;
    }

}
