package com.sohu.dc.jobkeeper.helper;

import org.apache.zookeeper.KeeperException;

public interface ZooKeeperOperation {
    
    public boolean execute() throws KeeperException, InterruptedException;
}
