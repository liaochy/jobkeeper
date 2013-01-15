package com.sohu.dc.jobkeeper.runtime;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.sohu.dc.jobkeeper.JConstants;
import com.sohu.dc.jobkeeper.exec.CmdLineExec;
import com.sohu.dc.jobkeeper.exec.CmdLineInterface;
import com.sohu.dc.jobkeeper.tools.DateUtil;
import com.sohu.dc.zkutil.zookeeper.ZKUtil;

public class JobDetail {
	private final static Log logger = LogFactory.getLog(JobDetail.class);

	private JobContext context;

	public JobDetail(JobContext context) {
		this.context = context;
	}

	protected final ArrayBlockingQueue<ShellBean> shellQueue = new ArrayBlockingQueue<ShellBean>(
			150);
	private Map<String, String> runNode = new ConcurrentHashMap<String, String>();

	private boolean start = false;

	/**
	 * 任务每小时执行一次
	 * 
	 * @throws IOException
	 */
	public void start() throws IOException {
		if (!start) {
			new ShellThread("shellThread").start();
			start = true;
		}
		runNode.clear();
		String p_hour = DateUtil.getDateTime("yyyyMMddHH",
				DateUtil.add(new Date(), Calendar.HOUR_OF_DAY, -1));
		String c_day = DateUtil.getDateTime("yyyyMMdd", new Date());
		String c_hour = DateUtil.getDateTime("HH", new Date());
		context.zooKeeper.createNode(
				ZKUtil.joinZNode(context.zkhome, JConstants.NODE_NODE, p_hour),
				CreateMode.PERSISTENT);
		context.zooKeeper.createNode(
				ZKUtil.joinZNode(context.zkhome, JConstants.NODE_NODE, c_day),
				CreateMode.PERSISTENT);
		watchNode(p_hour, c_hour);
		runEtlNode(p_hour, c_hour);
	}
	
	/**
	 * 任务每5分钟执行一次
	 * 分钟级别的任务，直接处理日志进行分析，不会有依赖
	 * 
	 * @throws IOException
	 */
	public void startMin() throws IOException {
		List<Node> nodes = context.getMinNodes();
		for (Node node : nodes) {
			CmdLineInterface cmdLine = new CmdLineExec(node, "");
			cmdLine.exec();
		}
	}

	/**
	 * watch所有要依赖的节点
	 * 
	 * @param p_hour
	 * @param c_hour
	 * @throws IOException
	 */
	private void watchNode(String p_hour, String c_hour) throws IOException {
		String p_day = DateUtil.getDateTime("yyyyMMdd",
				DateUtil.add(new Date(), Calendar.DAY_OF_MONTH, -1));
		List<Node> watcherNodes = context.getWatcherNodes(c_hour);
		boolean exists = false;
		for (Node watcher : watcherNodes) {
			String hourorday = p_hour;
			if ("day".equals(watcher.getType())) {
				hourorday = p_day;
			}
			String path = ZKUtil.joinZNode(context.zkhome,
					JConstants.NODE_NODE, hourorday, watcher.getName());
			exists = context.zooKeeper.ensureExists(path, new ExistsWatcher(
					new ShellBean(watcher.getName(), p_hour, p_day, c_hour)));
			if (exists) {
				context.zooKeeper.setData(path);
			}
		}
	}

	/**
	 * 首先运行没有依赖的节点
	 * 
	 * @param c_hour
	 */
	private void runEtlNode(String p_hour, String c_hour) {
		List<Node> nodes = context.getEtlNodes(c_hour);
		for (Node node : nodes) {
			CmdLineInterface cmdLine = new CmdLineExec(node, p_hour);
			cmdLine.exec();
		}
	}

	class ShellThread extends Thread {

		public ShellThread(String name) {
			super(name);
		}

		/**
		 * 从队列中取数据
		 */
		public void run() {
			while (true) {
				try {
					ShellBean shellBean = shellQueue.take();
					String suffix = shellBean.getSuffix();
					if (suffix.length() > 8) {
						getRunningNodeAndRun(shellBean);
					}
				} catch (InterruptedException e1) {
					logger.error("JobStart ShellThread InterruptedException ",
							e1);
				}
			}
		}

		/**
		 * 寻找所有依赖该节点的节点判断是否可运行
		 * 
		 * @param shellBean
		 */
		private void getRunningNodeAndRun(ShellBean shellBean) {
			List<Node> nodes = context.getNodesWithDependencys(
					shellBean.getName(), shellBean.getCurHour());

			String root = ZKUtil
					.joinZNode(context.zkhome, JConstants.NODE_NODE);
			List<String> nodeNames = context.zooKeeper.getNode(root + "/"
					+ shellBean.getSuffix(), null);
			List<String> dayNodes = context.zooKeeper.getNode(root + "/"
					+ shellBean.getDate(), null);
			for (Node node : nodes) {
				List<Dependency> dependencys = node.getDependency();
				int dependencyNum = 0;
				for (Dependency dependency : dependencys) {
					String name = dependency.getName();
					boolean containNode = false;
					if ("day".equals(dependency.getType())) {
						containNode = containsNode(name, dayNodes);
					} else {
						containNode = containsNode(name, nodeNames);
					}
					if (containNode) {
						dependencyNum++;
					}
				}
				if (dependencyNum == dependencys.size()) {
					runNode(node, shellBean);
				}
			}
		}

		/**
		 * 依赖的节点是否跑完
		 * 
		 * @param name
		 * @param nodes
		 * @return
		 */
		private boolean containsNode(String name, List<String> nodes) {
			for (String zooNodeName : nodes) {
				if (name.equals(zooNodeName)) {
					return true;
				}
			}
			return false;
		}

		/**
		 * 所依赖的节点跑完后运行该节点
		 * 
		 * @param node
		 * @param shellBean
		 */
		private void runNode(Node node, ShellBean shellBean) {
			String hourorday = shellBean.getSuffix();
			if ("day".equals(node.getType())) {
				hourorday = shellBean.getDate();
			}
			if (!runNode.containsKey(node.getName() + hourorday)) {
				runNode.put(node.getName() + hourorday, "1");
				CmdLineInterface cmdLine = new CmdLineExec(node, hourorday);
				cmdLine.exec();
			}
		}
	}

	class ExistsWatcher implements Watcher {
		private final ShellBean shellBean;

		public ExistsWatcher(ShellBean shellBean) {
			this.shellBean = shellBean;
		}

		public void process(WatchedEvent event) {
			String path = event.getPath();
			logger.info("path=" + path);
			try {
				shellQueue.put(shellBean);
			} catch (InterruptedException e) {
				logger.error("ExistsWatcher InterruptedException", e);
			}
			logger.info("receive notice : name=" + shellBean.getName()
					+ " suffix=" + shellBean.getSuffix());
		}
	}
}
