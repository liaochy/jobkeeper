package com.sohu.dc.jobkeeper.helper;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


class ProtocolSupport {
    private static final Logger LOG = Logger.getLogger(ProtocolSupport.class);

    protected final ZooKeeper zookeeper;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private long retryDelay = 500L;
    private int retryCount = 5;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public ProtocolSupport(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }
    
    /**
     * return zookeeper client instance
     * @return zookeeper client instance
     */
    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    /**
     * return the acl its using
     * @return the acl.
     */
    public List<ACL> getAcl() {
        return acl;
    }

    /**
     * set the acl 
     * @param acl the acl to set to
     */
    public void setAcl(List<ACL> acl) {
        this.acl = acl;
    }

    /**
     * get the retry delay in milliseconds
     * @return the retry delay
     */
    public long getRetryDelay() {
        return retryDelay;
    }

    /**
     * Sets the time waited between retry delays
     * @param retryDelay the retry delay
     */
    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    /**
     * Allow derived classes to perform 
     * some custom closing operations to release resources
     */
    protected void doClose() {
    }


    /**
     * Perform the given operation, retrying if the connection fails
     * @return object. it needs to be cast to the callee's expected 
     * return type.
     */
    protected Object retryOperation(ZooKeeperOperation operation) 
        throws KeeperException, InterruptedException {
        KeeperException exception = null;
        for (int i = 0; i < retryCount; i++) {
            try {
                return operation.execute();
            } catch (KeeperException.SessionExpiredException e) {
                LOG.warn("Session expired for: " + zookeeper + " so reconnecting due to: " + e, e);
                throw e;
            } catch (KeeperException.ConnectionLossException e) {
                if (exception == null) {
                    exception = e;
                }
                LOG.debug("Attempt " + i + " failed with connection loss so " +
                		"attempting to reconnect: " + e, e);
                retryDelay(i);
            }
        }
        throw exception;
    }

    /**
     * Ensures that the given path exists with no data, the current
     * ACL and no flags
     * @param path
     */
    protected void ensurePathExists(String path,CreateMode createMode) {
        ensureExists(path, null, acl, createMode);
    }
    

    /**
     * Ensures that the given path exists with no data, the current
     * ACL and no flags
     * @param path
     */
    protected void ensurePathExists(String path,byte[] data ,CreateMode createMode) {
        ensureExists(path, data, acl, createMode);
    }

    /**
     * Ensures that the given path exists with the given data, ACL and flags
     * @param path
     * @param acl
     * @param flags
     */
    protected void ensureExists(final String path, final byte[] data,
            final List<ACL> acl, final CreateMode flags) {
        try {
            retryOperation(new ZooKeeperOperation() {
                public boolean execute() throws KeeperException, InterruptedException {
                    Stat stat = zookeeper.exists(path, false);
                    if (stat != null) {
                        return true;
                    }
                    zookeeper.create(path, data, acl, flags);
                    return true;
                }
            });
        } catch (KeeperException e) {
            LOG.warn("Caught: " + e, e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOG.warn("Caught: " + e, e);
            e.printStackTrace();
        }
    }

    /**
     * Returns true if this protocol has been closed
     * @return true if this protocol is closed
     */
    protected boolean isClosed() {
        return closed.get();
    }

    /**
     * Performs a retry delay if this is not the first attempt
     * @param attemptCount the number of the attempts performed so far
     */
    protected void retryDelay(int attemptCount) {
        if (attemptCount > 0) {
            try {
                Thread.sleep(attemptCount * retryDelay);
            } catch (InterruptedException e) {
                LOG.debug("Failed to sleep: " + e, e);
            }
        }
    }
}
