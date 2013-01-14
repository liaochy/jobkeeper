package com.sohu.dc.jobkeeper;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.sohu.dc.jobkeeper.tools.DateUtil;
import com.sohu.dc.zkutil.Server;
import com.sohu.dc.zkutil.ZooKeeperConnectionException;
import com.sohu.dc.zkutil.conf.Configuration;
import com.sohu.dc.zkutil.conf.Constants;
import com.sohu.dc.zkutil.util.Sleeper;
import com.sohu.dc.zkutil.zookeeper.ZKUtil;
import com.sohu.dc.zkutil.zookeeper.ZooKeeperWatcher;

public class Jobkeeper implements Server {

	private static final Log LOG = LogFactory.getLog(Jobkeeper.class.getName());

	// MASTER is name of the webapp and the attribute name used stuffing this
	// instance into web context.
	private String name = "";
	// The configuration for the Master
	private final Configuration conf;

	private volatile boolean stopped = false;
	// Set on abort -- usually failure of our zk session.
	private volatile boolean abort = false;
	// flag set after we become the active master (used for testing)
	private volatile boolean isActiveMaster = false;

	// Our zk client.
	private ZooKeeperWatcher zooKeeper;
	// Manager and zk listener for master election
	private ActiveMasterManager activeMasterManager;

	public Jobkeeper(Configuration conf) throws IOException, KeeperException,
			InterruptedException {
		this.conf = new Configuration(conf);
		this.name = conf.get(JConstants.MASTER_ID_KEY);
		this.zooKeeper = new ZooKeeperWatcher(conf, this.name, this,
				JConstants.base_nodes);
	}

	public static void main(String[] args) throws Exception {
		new JobkeeperCommandLine().doMain(args);
	}

	public boolean isActiveMaster() {
		return this.activeMasterManager.isActiveMaster();
	}

	private static void stallIfBackupMaster(final Configuration conf,
			final ActiveMasterManager amm) throws InterruptedException {
		// If we're a backup master, stall until a primary to writes his address
		if (!conf.getBoolean(JConstants.MASTER_BACKUP_KEY, false)) {
			return;
		}
		LOG.debug("HMaster started in backup mode.  "
				+ "Stalling until master znode is written.");
		while (!amm.isActiveMaster()) {
			LOG.debug("Waiting for master address ZNode to be written "
					+ "(Also watching cluster state node)");
			Thread.sleep(conf.getInt(Constants.ZK_SESSION_TIMEOUT,
					Constants.DEFAULT_ZK_SESSION_TIMEOUT));
		}
	}

	public void start() {
		try {
			becomeActiveMaster();
			if (!this.stopped) {
				finishInitialization();
				loop();
			}
		} catch (Throwable t) {
			t.printStackTrace();
			abort("Unhandled exception. Starting shutdown.", t);
		} finally {

			if (this.activeMasterManager != null)
				this.activeMasterManager.stop();
			this.zooKeeper.close();
		}
		LOG.info("JobKeeper main thread exiting");
	}

	/**
	 * Try becoming active master.
	 * 
	 * @param startupStatus
	 * @return True if we could successfully become the active master.
	 * @throws InterruptedException
	 */
	private boolean becomeActiveMaster() throws InterruptedException {
		this.activeMasterManager = new ActiveMasterManager(zooKeeper, this);
		this.zooKeeper.registerListener(activeMasterManager);
		stallIfBackupMaster(this.conf, this.activeMasterManager);
		return this.activeMasterManager.blockUntilBecomingActiveMaster();
	}

	public void abort(String msg, Throwable t) {
		if (abortNow(msg, t)) {
			if (t != null)
				LOG.fatal(msg, t);
			else
				LOG.fatal(msg);
			this.abort = true;
			stop("Aborting");
		}
	}

	public boolean isAborted() {
		return this.abort;
	}

	public void finishInitialization() throws ZooKeeperConnectionException {
		String p_day = DateUtil.getDateTime("yyyyMMdd",
				DateUtil.add(new Date(), Calendar.DAY_OF_MONTH, -1));
		zooKeeper.createZNodes(ZKUtil.joinZNode(JConstants.NODE_NODE, p_day));
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"applicationContext.xml");
		JobStart jobStart = (JobStart) context.getBean("jobStart");
		jobStart.setJobkeeper(this);
	}

	// Check if we should stop every second.
	private Sleeper stopSleeper = new Sleeper(1000, this);

	private void loop() {
		while (!this.stopped) {
			stopSleeper.sleep();
		}
	}

	public void stop(String why) {
		LOG.info(why);
		this.stopped = true;
		// We wake up the stopSleeper to stop immediately
		stopSleeper.skipSleepCycle();
		// If we are a backup master, we need to interrupt wait
		if (this.activeMasterManager != null) {
			synchronized (this.activeMasterManager.clusterHasActiveMaster) {
				this.activeMasterManager.clusterHasActiveMaster.notifyAll();
			}
		}
	}

	private boolean abortNow(final String msg, final Throwable t) {
		if (!this.isActiveMaster) {
			return true;
		}
		if (t != null && t instanceof KeeperException.SessionExpiredException) {
			try {
				LOG.info("Primary Master trying to recover from ZooKeeper session "
						+ "expiry.");
				return !tryRecoveringExpiredZKSession();
			} catch (Throwable newT) {
				LOG.error(
						"Primary master encountered unexpected exception while "
								+ "trying to recover from ZooKeeper session"
								+ " expiry. Proceeding with server abort.",
						newT);
			}
		}
		return true;
	}

	/**
	 * We do the following in a different thread. If it is not completed in
	 * time, we will time it out and assume it is not easy to recover.
	 * 
	 * 1. Create a new ZK session. (since our current one is expired) 2. Try to
	 * become a primary master again 3. Initialize all ZK based system trackers.
	 * 4. Assign root and meta. (they are already assigned, but we need to
	 * update our internal memory state to reflect it) 5. Process any RIT if any
	 * during the process of our recovery.
	 * 
	 * @return True if we could successfully recover from ZK session expiry.
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws KeeperException
	 * @throws ExecutionException
	 */
	private boolean tryRecoveringExpiredZKSession()
			throws InterruptedException, IOException, KeeperException,
			ExecutionException {

		this.zooKeeper = new ZooKeeperWatcher(conf, this.name, this,
				JConstants.base_nodes);

		Callable<Boolean> callable = new Callable<Boolean>() {
			public Boolean call() throws InterruptedException, IOException,
					KeeperException {
				if (!becomeActiveMaster()) {
					return Boolean.FALSE;
				}
				return Boolean.TRUE;
			}
		};

		long timeout = conf.getLong("master.zksession.recover.timeout", 300000);
		java.util.concurrent.ExecutorService executor = Executors
				.newSingleThreadExecutor();
		Future<Boolean> result = executor.submit(callable);
		executor.shutdown();
		if (executor.awaitTermination(timeout, TimeUnit.MILLISECONDS)
				&& result.isDone()) {
			Boolean recovered = result.get();
			if (recovered != null) {
				return recovered.booleanValue();
			}
		}
		executor.shutdownNow();
		return false;
	}

	public boolean isStopped() {
		return this.stopped;
	}

	public Configuration getConfiguration() {
		return this.conf;
	}

	public ZooKeeperWatcher getZooKeeper() {
		return this.zooKeeper;
	}

	public String getServerName() {
		return this.name;
	}
}
