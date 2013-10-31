package com.sohu.dc.jobkeeper.tools;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.sohu.dc.jobkeeper.JConstants;
import com.sohu.dc.jobkeeper.helper.ZooKeeperHelper;
import com.sohu.dc.zkutil.conf.Configuration;
import com.sohu.dc.zkutil.conf.Constants;
import com.sohu.dc.zkutil.zookeeper.ZKUtil;

public class JobKeeperTool {
	private static final Log logger = LogFactory.getLog("TOOL");
	private String root_home;
	Object lock = new Object();

	public JobKeeperTool(String home) {
		this.root_home = home;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		logger.info("args : " + StringUtils.join(args, ","));
		Configuration conf = new Configuration();
		if (args.length == 2) {
			JobKeeperTool jobKeeper = new JobKeeperTool(
					conf.get(Constants.ZOOKEEPER_ZNODE_PARENT));
			jobKeeper.addNode(args[0], args[1], true);
		} else if (args.length >= 3) {
			JobKeeperTool jobKeeper = new JobKeeperTool(
					conf.get(Constants.ZOOKEEPER_ZNODE_PARENT));
			String[] others = new String[args.length - 2];
			System.arraycopy(args, 2, others, 0, others.length);
			jobKeeper.opConfNode(args[0], args[1], others);
		} else {
			System.exit(0);
		}
	}

	public int addNode(String nodeName, String suffix) {
		return addNode(nodeName, suffix, false);
	}

	public int addNode(String nodeName, String suffix, boolean closeSession) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		boolean exist = zooKeeper
				.ensureExists(ZKUtil.joinZNode(root_home, JConstants.NODE_NODE,
						suffix), null);
		if (!exist) {
			zooKeeper.createNode(
					ZKUtil.joinZNode(root_home, JConstants.NODE_NODE, suffix),
					CreateMode.PERSISTENT);
		}
		String node = ZKUtil.joinZNode(root_home, JConstants.NODE_NODE, suffix,
				nodeName);
		zooKeeper.createNode(node, CreateMode.PERSISTENT);
		logger.info("create node = " + node);
		if (closeSession) {
			zooKeeper.closeConnection();
		}
		return 1;

	}

	public void opConfNode(String type, String nodeName, String... jsons) {
		String json = StringUtils.join(jsons, " ");
		if ("add".equals(type)) {
			addConfNode(nodeName, json);
		} else if ("del".equals(type)) {
			delConfNode(nodeName, json);
		} else if ("up".equals(type)) {
			upConfNode(nodeName, json);
		} else if ("warn".equals(type)) {
			addWarnNode(nodeName, json);
		} else if ("clearnode".equals(type)) {
			if ("hour".equals(nodeName)) {
				clearHourNode(Integer.parseInt(json));
			} else {
				clearDayNode(Integer.parseInt(json));
			}
		} else if ("block".equals(type)) {
			blockUtilAddNode(nodeName, json);
		}

	}

	private void blockUtilAddNode(String date, String nodeName) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		String node = ZKUtil.joinZNode(root_home, JConstants.NODE_NODE, date,
				nodeName);
		boolean exist = zooKeeper.ensureExists(node, new ExistsWatcher());
		if (!exist) {
			synchronized (lock) {
				try {
					lock.wait(3600000);
				} catch (InterruptedException e) {
					logger.info("blockUtilAddNode Interrupted");
				}
			}
		}
		zooKeeper.closeConnection();
	}

	class ExistsWatcher implements Watcher {
		public void process(WatchedEvent event) {
			synchronized (lock) {
				lock.notify();
			}
		}
	}

	private void addWarnNode(String nodeName, String json) {
		if ("10".equals(json) || "11".equals(json) || "130".equals(json)) {
			return;
		}
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		String mobiles = zooKeeper.getData(
				ZKUtil.joinZNode(root_home, JConstants.NODE_MOBILE), null);
		String[] mobileArray = mobiles.split(",");
		String content = "FAILED: Hive Internal Error: code=" + json;
		if ("zookeeper".equals(nodeName)) {
			content = json;
		} else {
			zooKeeper.createNode(
					ZKUtil.joinZNode(root_home, JConstants.NODE_WARN, "warn-"),
					json.getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
		}
		for (String mobile : mobileArray) {
			SendMsg s = new SendMsg();
			s.sendMsg(mobile, content);
		}
		// true保持连接
		if (!"true".equals(nodeName) && !"zookeeper".equals(nodeName)) {
			zooKeeper.closeConnection();
		}
	}

	private void clearDayNode(int count) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		for (int i = count; i > 2; i--) {
			String p_day = DateUtil.getDateTime("yyyyMMdd",
					DateUtil.add(new Date(), Calendar.DAY_OF_MONTH, -i));
			zooKeeper.deleteNode(ZKUtil.joinZNode(root_home,
					JConstants.NODE_NODE, p_day));
		}
		zooKeeper.closeConnection();
	}

	private void clearHourNode(int count) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		for (int i = count; i > 50; i--) {
			String p_hour = DateUtil.getDateTime("yyyyMMddHH",
					DateUtil.add(new Date(), Calendar.HOUR_OF_DAY, -i));
			zooKeeper.deleteNode(ZKUtil.joinZNode(root_home,
					JConstants.NODE_NODE, p_hour));
		}
		zooKeeper.closeConnection();
	}

	private void addConfNode(String nodeName, String json) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		zooKeeper.createNode(nodeName, json.getBytes(), CreateMode.PERSISTENT);
		zooKeeper.closeConnection();
	}

	private void delConfNode(String nodeName, String json) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		zooKeeper.deleteNode(nodeName);
		zooKeeper.closeConnection();
	}

	private void upConfNode(String nodeName, String json) {
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		zooKeeper.setData(nodeName, json.getBytes());
		zooKeeper.closeConnection();
	}
}
