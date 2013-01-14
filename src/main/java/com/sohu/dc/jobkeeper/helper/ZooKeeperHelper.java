package com.sohu.dc.jobkeeper.helper;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperHelper {

	public static void main(String[] args) {
		System.setProperty("zk", "10.10.83.190:2181");
		ZooKeeperHelper zk1 = new ZooKeeperHelper();
		List<String> nodes = zk1.getNode("/jobkeeper/10_10_83_190/node", null);
		HashMap<String, String> map = new HashMap<String, String>();
		for (String node : nodes) {
			String data = zk1.getData("/jobkeeper/10_10_83_190/node/" + node,
					null);
			map.put(node, data);
		}
		System.setProperty("zk",
				"10.11.152.97:2181,10.11.152.98:2181,10.11.152.99:2181");
		ZooKeeperClient.clearZK();
		ZooKeeperHelper zk2 = new ZooKeeperHelper();
		for (Entry<String, String> entry : map.entrySet()) {
			zk2.createNode("/jobkeeper/sohuwl_190/job/"+entry.getKey(), entry.getValue().getBytes(),CreateMode.PERSISTENT);
		}
	}

	public void closeConnection() {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, "", null);
		leader.close();
	}

	public void createNode(String nodeName, CreateMode createMode) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		leader.createNode(createMode);
	}

	public void createNodeRecur(String nodeName, CreateMode createMode) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		leader.createPath(createMode);
	}

	public void createNode(String nodeName, byte[] data, CreateMode createMode) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		leader.createNode(createMode, data);
	}

	public List<String> getNode(String nodeName, Watcher watcher) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		return leader.getNode(watcher);
	}

	public void deleteNode(String nodeName) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		leader.deleteNode();
	}

	public boolean ensureExists(String nodeName, Watcher watcher) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		return leader.ensureExists(watcher);
	}

	public boolean ensureExists(String nodeName, boolean watcher) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		return leader.ensureExists(watcher);
	}

	public String getData(String nodeName, Watcher watcher) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		return leader.getData(watcher);
	}

	public void setData(String nodeName) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		leader.setData();
	}

	public void setData(String nodeName, byte[] data) {
		ZooKeeper zookeeper = ZooKeeperClient.getZooKeeper();
		NodeManager leader = new NodeManager(zookeeper, nodeName, null);
		leader.setData(data);
	}
}
