package com.sohu.dc.jobkeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sohu.dc.jobkeeper.helper.ZooKeeperHelper;
import com.sohu.dc.jobkeeper.runtime.JobContext;
import com.sohu.dc.jobkeeper.runtime.JobDetail;
import com.sohu.dc.jobkeeper.runtime.Node;
import com.sohu.dc.jobkeeper.tools.DateUtil;
import com.sohu.dc.jobkeeper.tools.JobKeeperTool;
import com.sohu.dc.zkutil.conf.Configuration;
import com.sohu.dc.zkutil.conf.Constants;
import com.sohu.dc.zkutil.util.Threads;
import com.sohu.dc.zkutil.zookeeper.ZKUtil;

public class JobStart {
	private final static Log logger = LogFactory.getLog(JobStart.class);

	private Jobkeeper jobkeeper;
	private JobContext context;
	private JobDetail job;

	public void setJobkeeper(Jobkeeper jobkeeper) {
		this.jobkeeper = jobkeeper;
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		this.context = new JobContext(zooKeeper,
				jobkeeper.getZooKeeper().baseZNode);
		this.job = new JobDetail(context);
	}

	private boolean canRun() {
		Configuration conf = jobkeeper.getConfiguration();
		int retrycount = conf.getInt(JConstants.BACKUP_MASTER_TRY_COUNT, 3);
		int waittime = conf.getInt(Constants.ZK_SESSION_TIMEOUT,
				Constants.DEFAULT_ZK_SESSION_TIMEOUT);
		for (int i = 0; i < retrycount && !jobkeeper.isActiveMaster(); i++) {
			Threads.sleep(waittime);
		}
		return jobkeeper.isActiveMaster();

	}

	/**
	 * 任务每小时执行一次
	 * 
	 * @throws IOException
	 */
	public void jobStart() throws IOException {
		if (!canRun()) {
			logger.info("current jobkeeper is not master !");
			return;
		}
		logger.info("job start .");
		this.context.createContext();
		this.job.start();
	}

	/**
	 * 任务每5分钟执行一次 分钟级别的任务，直接处理日志进行分析，不会有依赖
	 * 
	 * @throws IOException
	 */
	public void jobStartMin() throws IOException {
		if (!canRun()) {
			logger.info("current jobkeeper is not master !");
			return;
		}
		logger.info("min job start .");
		String min = DateUtil.getDateTime("m", new Date());
		this.context.createContextMin(min);
		this.job.startMin();
	}

	public static void main(String[] args) {
		int toExecute = Integer.parseInt("00");
		if (10 <= toExecute) {
			System.out.println("ok");
		}
	
	}

	public void checkJobsAndWarn() throws IOException {
		List<Node> nodes = this.context.getAllNodes();

		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		Configuration conf = new Configuration();
		String p_hour = DateUtil.getDateTime("yyyyMMddHH",
				DateUtil.add(new Date(), Calendar.HOUR_OF_DAY, -1));
		String home = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT);
		String path = ZKUtil.joinZNode(home, JConstants.NODE_NODE, p_hour);
		String dayPath = ZKUtil.joinZNode(
				home,
				JConstants.NODE_NODE,
				DateUtil.getDateTime("yyyyMMdd",
						DateUtil.add(new Date(), Calendar.DAY_OF_WEEK, -1)));

		List<String> existHourNodes = zooKeeper.getNode(path, null);
		List<String> existDayNodes = zooKeeper.getNode(dayPath, null);

		Iterator<Node> it = nodes.iterator();
		while (it.hasNext()) {
			Node node = it.next();
			if (node.getType().equals("hour")
					&& existHourNodes.contains(node.getName())) {
				it.remove();
				continue;
			}
			if (node.getType().equals("day")
					&& existDayNodes.contains(node.getName())) {
				it.remove();
				continue;
			}
			int cur = Integer.parseInt(DateUtil.getDateTime("HH", new Date()));
			if (node.getType().equals("day")) {
				int toExecute = Integer.parseInt(node.getHour());
				if (cur <= toExecute) {
					it.remove();
					continue;
				}
			}
		}
		if (nodes.size() > 0) {
			List<String> unready = new ArrayList<String>();
			for (Node node : nodes) {
				unready.add(node.getName());
			}
			String warn = StringUtils.join(unready, ",");
			JobKeeperTool jobkeeper = new JobKeeperTool(home);

			jobkeeper.opConfNode("warn", "zookeeper", p_hour + ": " + warn
					+ " not finish!");
		}
	}

	/**
	 * 半小时判断kpi是否完成，否则告警
	 * 
	 * @throws IOException
	 */
	public void checkJobs() throws IOException {
		if (!canRun()) {
			logger.info("current jobkeeper is not master !");
			return;
		}
		ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
		Configuration conf = new Configuration();
		String p_hour = DateUtil.getDateTime("yyyyMMddHH",
				DateUtil.add(new Date(), Calendar.HOUR_OF_DAY, -1));
		String home = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT);
		String path = ZKUtil.joinZNode(home, JConstants.NODE_NODE, p_hour,
				"kpi");
		boolean exists = zooKeeper.ensureExists(path, null);
		if (!exists) {
			JobKeeperTool jobkeeper = new JobKeeperTool(home);
			String identity = conf.get(JConstants.MASTER_ID_KEY);
			jobkeeper.opConfNode("warn", "true", identity + " " + p_hour
					+ "kpi not finish!");
		}

	}

}
