package com.sohu.dc.jobkeeper;

import java.util.Arrays;
import java.util.List;

public class JConstants {

	public static final String MASTER_BACKUP_KEY = "jobkeeper.master.backup";
	public static final boolean MASTER_BACKUP = false;

	public static final String BACKUP_MASTER_TRY_COUNT = "master.backup.try.count";
	
	public static final String MASTER_ID_KEY = "jobkeeper.master.jobkeeperid";

	public static final String NODE_MASTER = "master";
	public static final String NODE_MOBILE = "mobile";
	public static final String NODE_NODE = "node";
	public static final String NODE_JOB = "job";
	public static final String NODE_HOME = "scripts_home";
	public static final String NODE_WARN = "warn";
	public static final String NODE_BACKUP = "backup-masters";

	public static final List<String> base_nodes = Arrays.asList(NODE_MOBILE,
			NODE_NODE, NODE_JOB, NODE_HOME, NODE_WARN,NODE_BACKUP);
}
