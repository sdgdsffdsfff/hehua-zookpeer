package com.hehua.framework.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public abstract class ZookeeperBasedObjectHolder<T> implements Watcher {

    protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    protected volatile ZooKeeper keeper;

    private volatile T object;

    /**
     * @param bizName
     */
    protected ZookeeperBasedObjectHolder() {
        try {
            keeper = ZookeeperClientFactory.getInstance().createClient(this);
        } catch (Throwable e) {
            logger.error("Ops.", e);
            throw new RuntimeException(e);
        }
    }

    protected T getObject() {
        if (object == null) {
            synchronized (this) {
                if (object == null) {
                    object = initObject();
                }
            }
        }
        return object;
    }

    protected abstract T initObject();

    protected abstract T rebuildObject(String path);

    protected abstract boolean needRebuild(String path);

    protected abstract void destroyOldObject(T obj);

    protected long waitBeforceOldClosed() {
        return 0;
    }

    @SuppressWarnings("incomplete-switch")
    @Override
    public void process(WatchedEvent event) {
        boolean needCheck = false;
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    break;
                case Expired:
                    logger.warn("Zookeeper session expired:" + event);
                    ZooKeeper oldOne = keeper;
                    try {
                        keeper = ZookeeperClientFactory.getInstance().createClient(this);
                        oldOne.close();
                    } catch (Exception e) {
                        logger.error("fail to create zookeeper.", e);
                    }
                    needCheck = true;
                    break;
            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged
                || event.getType() == Event.EventType.NodeDataChanged) {
            needCheck = true;
        } else if (event.getType() == EventType.NodeCreated) {
            logger.info("need not deal event:" + event);
        } else {
            logger.warn("Unhandled event:" + event);
        }
        if (needCheck) {
            if (needRebuild(event.getPath())) {
                T newOne = rebuildObject(event.getPath());
                if (newOne != null) {
                    T oldOne = object;
                    object = newOne;
                    if (waitBeforceOldClosed() > 0) {
                        try {
                            Thread.sleep(waitBeforceOldClosed());
                        } catch (InterruptedException e) {
                            // donothing.
                        }
                    }
                    destroyOldObject(oldOne);
                }
            }
        }
    }
}
