/**
 * 
 */
package com.hehua.framework.config;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.helpers.LogLog;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.hehua.framework.zookeeper.ZookeeperClientFactory;

public final class ZookeeperConfigManager implements Watcher, ConfigManager {

    private static final int INIT_FAIL_RETRY_TIMES = 10;

    private static final long INIT_FAIL_RETRY_DELAY = 200;

    private static final int RELOAD_FAIL_RETRY_TIMES = 3;

    private static final long RELOAD_FAIL_RETRY_DELAY = 1000;

    //    private final Log logger = LogFactory.getLog(getClass());

    private static final String CONFIGURE_PATH = "/hehua/config";

    private volatile Map<String, String> configure;

    private ZooKeeper keeper = null;

    private ZookeeperConfigManager() {
        boolean init = initWithRetry(INIT_FAIL_RETRY_TIMES, INIT_FAIL_RETRY_DELAY);
        if (!init) {
            throw new RuntimeException("fail to init zookeeper config.");
        }
    }

    public boolean initWithRetry(int maxRetryTimes, long retryDelay) {
        boolean init = false;
        int retryTimes = 0;
        while ((!init) && (retryTimes++) < maxRetryTimes) {
            try {
                configure = reloadAll();
                init = true;
            } catch (Throwable e) {
                LogLog.debug("Ops. init config from zk err", e);
            }
        }
        return init;
    }

    private static ZookeeperConfigManager _instance = new ZookeeperConfigManager();

    public static ZookeeperConfigManager getInstance() {
        return _instance;
    }

    @SuppressWarnings("incomplete-switch")
    @Override
    public void process(WatchedEvent event) {
        LogLog.debug("zookeeper event " + event.getType() + ", " + event.getState() + ", "
                + event.getPath());
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    break;
                case Expired:
                    // TODO 这里应该重新初始化zk
                    LogLog.error("Zookeeper session expired:" + event);
                    LogLog.debug("re-initializing ZooKeeper");
                    if (!initWithRetry(RELOAD_FAIL_RETRY_TIMES, RELOAD_FAIL_RETRY_DELAY)) {
                        LogLog.error("Ops. initConfig from zookeeper Failure.");
                    }
                    break;
            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged
                || event.getType() == Event.EventType.NodeDataChanged) {
            if (StringUtils.startsWith(event.getPath(), CONFIGURE_PATH)) {
                if (!initWithRetry(RELOAD_FAIL_RETRY_TIMES, RELOAD_FAIL_RETRY_DELAY)) {
                    LogLog.error("Ops. initConfig from zookeeper Failure.");
                } else {
                    LogLog.debug("zookeeper config changed. reload completed, change:"
                            + event.getPath());
                }
            } else {
                LogLog.debug("zk node changed, but zkconfig no need change.");
            }

        } else if (event.getType() == EventType.NodeCreated) {
            LogLog.debug("need not deal event:" + event);
        } else {
            LogLog.warn("Unhandled event:" + event);
        }
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     * 
     */
    private Map<String, String> reloadAll() throws IOException, KeeperException,
            InterruptedException {
        boolean notInit = keeper == null;
        ZooKeeper old = null;
        if (notInit) {
            keeper = ZookeeperClientFactory.getInstance().createClient(this);
        } else {
            boolean oldIsAlive = keeper.getState().isAlive();
            if (!oldIsAlive) {
                old = keeper;
                keeper = ZookeeperClientFactory.getInstance().createClient(this);
            } else {
                old = null;
            }
        }
        List<String> children = keeper.getChildren(CONFIGURE_PATH, true);
        Map<String, String> result = new HashMap<String, String>();
        for (String path : children) {
            byte[] data = keeper.getData(CONFIGURE_PATH + "/" + path, true, null);
            String value = new String(data);
            result.put(path, value);
        }
        try {
            if (old != null) {
                old.close(); // 将旧的关闭掉
                LogLog.debug("success to close expired ZooKeeper:" + old.hashCode());
            }
        } catch (InterruptedException e) {
            LogLog.error("fail to close expired ZooKeeper:" + old.hashCode(), e);
        }
        return result;
    }

    @Override
    public String getString(String key) {
        return configure.get(key);
    }

    @Override
    public void setString(String key, String value) {
        try {
            Stat stat = new Stat();
            String fullkey = CONFIGURE_PATH + "/" + key;
            keeper.getData(fullkey, null, stat);
            keeper.setData(fullkey, value.getBytes(), stat.getVersion());
        } catch (Throwable e) {
            LogLog.error("error on set data," + key + "=[" + value + "].", e);
            throw new RuntimeException("error on set data," + key + "=[" + value + "].", e);
        }
    }

    @Override
    public Map<String, String> getAll() {
        return configure;
    }

    public static void main(String[] args) {

        if (args == null || args.length < 2) {
            System.err.println("ZookeeperConfigManager $configKey $configFile");
            return;
        }

        try {
            String configKey = args[0];
            String configFile = args[1];

            String configValue = FileUtils.readFileToString(new File(configFile));
            getInstance().setString(configKey, configValue);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
