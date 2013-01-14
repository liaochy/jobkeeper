package com.sohu.dc.jobkeeper.helper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.sohu.dc.zkutil.conf.Configuration;
import com.sohu.dc.zkutil.conf.Constants;

public class ZooKeeperClient {
	private static final Log LOG = LogFactory.getLog(ZooKeeperClient.class);
	private static ZooKeeper zookeeper = null;
	private static CountDownLatch latch;

	public static void clearZK() {
		zookeeper = null;
	}

	public static ZooKeeper getZooKeeper() {
		if (zookeeper == null) {
			synchronized (ZooKeeperClient.class) {
				if (zookeeper == null) {
					latch = new CountDownLatch(1);
					zookeeper = buildClient();
					try {
						latch.await(30, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						LOG.error("InterruptedException", e);
					} finally {
						latch = null;
					}
				}
			}
		}
		return zookeeper;
	}

	private static ZooKeeper buildClient() {
		Configuration conf = new Configuration();
		String quorum = conf.get(Constants.ZOOKEEPER_QUORUM);
		try {
			return new ZooKeeper(quorum, 30000, new SessionWatcher());
		} catch (IOException e) {
			throw new RuntimeException("init zookeeper fail.", e);
		}
	}

	static class SessionWatcher implements Watcher {

		public void process(WatchedEvent event) {
			LOG.debug("Received ZooKeeper Event, " + "type=" + event.getType()
					+ ", " + "state=" + event.getState() + ", " + "path="
					+ event.getPath());

			if (event.getState() == KeeperState.SyncConnected) {
				if (latch != null) {
					latch.countDown();
				}
			} else if (event.getState() == KeeperState.Expired) {
				LOG.info("Zookeeper Client Expired,trying to rebuild clietn.");
				close();
				getZooKeeper();
			}
		}
	}

	public static void close() {
		if (zookeeper != null) {
			try {
				zookeeper.close();
			} catch (InterruptedException e) {
				LOG.error("zookeeper client close Interrupted", e);
			}
			zookeeper = null;
		}
	}

	public static void main(String[] args) throws Exception {
		while (true) {
			ZooKeeper zk = ZooKeeperClient.getZooKeeper();
			long sessionId = zk.getSessionId();
			// close the old connection
			// to create a SESSION_EXPIRED exception
			new ZooKeeper(
					"10.11.152.97:2181,10.11.152.98:2181,10.11.152.99:2181",
					30000, null, sessionId, null).close();
			Thread.sleep(10000L);
			//
			// rebuild a new session
			long newSessionid = ZooKeeperClient.getZooKeeper().getSessionId();

			// check the new session
			String status = newSessionid != sessionId ? "OK" : "FAIL";
			System.out
					.printf("%s --> %s %s\n", sessionId, newSessionid, status);

			// close the client
			// ZooKeeperClient.getZooKeeper().close();
		}
	}
}
