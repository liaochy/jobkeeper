package com.sohu.dc.jobkeeper.runtime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sohu.dc.jobkeeper.JConstants;
import com.sohu.dc.jobkeeper.helper.ZooKeeperHelper;

public class JobContext {

	private static final Log log = LogFactory.getLog(JobContext.class);

	private List<Node> etlNodes = new ArrayList<Node>();

	public ZooKeeperHelper zooKeeper;

	public String zkhome;

	private static final String SCRIPTS_HOME = "${SCRIPTS_HOME}";

	private List<Node> otherNodes = new ArrayList<Node>();

	private List<Node> minNodes = new ArrayList<Node>();

	public List<Node> getMinNodes() {
		return minNodes;
	}

	/**
	 * 带有指定依赖的按小时的节点和指定小时按天的节点
	 * 
	 * @return
	 */
	public List<Node> getNodesWithDependencys(String nodeName, String c_hour) {
		List<Node> nodesWithDependencys = new ArrayList<Node>();
		for (Node node : otherNodes) {
			List<Dependency> dependencys = node.getDependency();
			for (Dependency dependency : dependencys) {
				if (nodeName.equals(dependency.getName())) {
					if ("day".equals(node.getType())) {
						if (!c_hour.equals(node.getHour())) {
							break;
						}
					}
					nodesWithDependencys.add(node);
					break;
				}
			}
		}
		return nodesWithDependencys;
	}

	/**
	 * 找到所有要watcher的节点
	 * 
	 * @return
	 */
	public List<Node> getWatcherNodes(String c_hour) {
		List<Node> nodesWithDependencys = new ArrayList<Node>();
		Set<Node> sortedNames = new HashSet<Node>();
		for (Node node : otherNodes) {
			if ("day".equals(node.getType())) {
				if (!c_hour.equals(node.getHour())) {
					continue;
				}
			}
			List<Dependency> dependencys = node.getDependency();
			for (Dependency dependency : dependencys) {
				Node tmpNode = new Node();
				tmpNode.setName(dependency.getName());
				tmpNode.setType(dependency.getType());
				sortedNames.add(tmpNode);
			}
		}
		nodesWithDependencys.addAll(sortedNames);
		return nodesWithDependencys;
	}

	/**
	 * 所有etl节点
	 * 
	 * @return
	 */
	public List<Node> getEtlNodes(String hour) {
		List<Node> nodesByDay = new ArrayList<Node>();
		for (Node node : etlNodes) {
			if ("day".equals(node.getType())) {
				if (!hour.equals(node.getHour())) {
					continue;
				}
			}
			nodesByDay.add(node);
		}
		return nodesByDay;
	}

	public JobContext(ZooKeeperHelper zooKeeper, String zkhome) {
		this.zkhome = zkhome;
		this.zooKeeper = zooKeeper;
	}

	public List<Node> getAllNodes() {

		List<Node> list = new ArrayList<Node>();
		List<String> nodeNames = zooKeeper.getNode(zkhome + "/"
				+ JConstants.NODE_JOB, null);

		String scripts_home = zooKeeper.getData(zkhome + "/"
				+ JConstants.NODE_HOME, null);
		for (String nodeName : nodeNames) {
			String nodeStr = zooKeeper.getData(zkhome + "/"
					+ JConstants.NODE_JOB + "/" + nodeName, null);
			Node node = parse(nodeStr, scripts_home);
			if ("minute".equals(node.getType())) {
				// do nothing
			} else if (node.getDependency().size() == 0) {
				list.add(node);
			} else {
				list.add(node);
			}
		}
		return list;
	}

	public void createContext() {
		etlNodes.clear();
		otherNodes.clear();
		List<String> nodeNames = zooKeeper.getNode(zkhome + "/"
				+ JConstants.NODE_JOB, null);

		String scripts_home = zooKeeper.getData(zkhome + "/"
				+ JConstants.NODE_HOME, null);
		for (String nodeName : nodeNames) {
			String nodeStr = zooKeeper.getData(zkhome + "/"
					+ JConstants.NODE_JOB + "/" + nodeName, null);
			Node node = parse(nodeStr, scripts_home);
			if ("minute".equals(node.getType())) {
				// do nothing
			} else if (node.getDependency().size() == 0) {
				etlNodes.add(node);
			} else {
				otherNodes.add(node);
			}
		}
		log.info("etlNodes are : " + StringUtils.join(etlNodes, ","));
		log.info("otherNodes are : " + StringUtils.join(otherNodes, ","));
	}

	/**
	 * 找出当前时间按分钟统计 需要运行的节点
	 * 
	 * @return
	 */
	public void createContextMin(String min) {
		minNodes.clear();
		List<String> nodeNames = zooKeeper.getNode(zkhome + "/"
				+ JConstants.NODE_JOB, null);

		String scripts_home = zooKeeper.getData(zkhome + "/"
				+ JConstants.NODE_HOME, null);
		for (String nodeName : nodeNames) {
			String nodeStr = zooKeeper.getData(zkhome + "/"
					+ JConstants.NODE_JOB + "/" + nodeName, null);
			Node node = parse(nodeStr, scripts_home);
			if ("minute".equals(node.getType())) {
				int canRun = Integer.parseInt(min)
						% Integer.parseInt(node.getMinute());
				if (canRun == 0) {
					minNodes.add(node);
				}
			}
		}
		log.info("minNodes are : " + StringUtils.join(minNodes, ","));
	}

	private static Node parse(String nodeStr, String scripts_home) {
		Node node = new Node();
		try {
			JSONObject jsonObject = new JSONObject(nodeStr);
			node.setName(jsonObject.getString("name"));
			String path = jsonObject.getString("path");
			if (path.startsWith(SCRIPTS_HOME)) {
				path = path.replace(SCRIPTS_HOME, scripts_home);
			}
			node.setPath(path);
			node.setType(jsonObject.getString("type"));
			node.setOwner(jsonObject.getString("owner"));
			if (!jsonObject.isNull("hour")) {
				node.setHour(jsonObject.getString("hour"));
			}
			if (!jsonObject.isNull("minute")) {
				node.setMinute(jsonObject.getString("minute"));
			}
			if (!jsonObject.isNull("argument")) {
				node.setArgument(jsonObject.getString("argument"));
			}
			if (!jsonObject.isNull("dependency")) {
				JSONArray array = jsonObject.getJSONArray("dependency");
				List<Dependency> dependencys = new ArrayList<Dependency>();
				for (int i = 0; i < array.length(); i++) {
					JSONObject dependency = array.getJSONObject(i);
					Dependency depend = new Dependency();
					depend.setName(dependency.getString("name"));
					depend.setType(dependency.getString("type"));
					dependencys.add(depend);
				}
				node.setDependency(dependencys);
			}
		} catch (JSONException e) {
		}
		return node;
	}

}
