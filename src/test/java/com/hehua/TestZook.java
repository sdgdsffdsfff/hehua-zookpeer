package com.hehua;

import com.hehua.framework.config.ZookeeperConfigManager;
import junit.framework.TestSuite;
import org.junit.Test;


/**
 * Created by hewenjerry on 14-9-17.
 */
public class TestZook extends TestSuite {

    @Test
    public void testMethod() {
        System.out.println("hello world");
    }

    @Test
    public void testConnectionClient() throws InterruptedException {
        ZookeeperConfigManager.getInstance();
        Thread.sleep(100000000000l);
    }
}
