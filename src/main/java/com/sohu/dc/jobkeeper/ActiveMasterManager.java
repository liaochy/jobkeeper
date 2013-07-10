/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sohu.dc.jobkeeper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.sohu.dc.zkutil.Server;
import com.sohu.dc.zkutil.conf.Configuration;
import com.sohu.dc.zkutil.conf.Constants;
import com.sohu.dc.zkutil.zookeeper.ZKUtil;
import com.sohu.dc.zkutil.zookeeper.ZooKeeperListener;
import com.sohu.dc.zkutil.zookeeper.ZooKeeperWatcher;

/**
 * Handles everything on master-side related to master election.
 * 
 * <p>
 * Listens and responds to ZooKeeper notifications on the master znode, both
 * <code>nodeCreated</code> and <code>nodeDeleted</code>.
 * 
 * <p>
 * Contains blocking methods which will hold up backup masters, waiting for the
 * active master to fail.
 * 
 * <p>
 * This class is instantiated in the HMaster constructor and the method
 * #blockUntilBecomingActiveMaster() is called to wait until becoming the active
 * master of the cluster.
 */
class ActiveMasterManager extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(ActiveMasterManager.class);

	final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);

	private final Server master;

	private String masterAddressZNode;
	private String backupMasterAddressZnode;

	/**
	 * @param watcher
	 * @param sn
	 *            ServerName
	 * @param master
	 *            In an instance of a Master.
	 */
	ActiveMasterManager(ZooKeeperWatcher watcher, Server master) {
		super(watcher);
		this.master = master;
		Configuration conf = master.getConfiguration();
		String baseZNode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT, Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
		this.masterAddressZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.master", "master"));
		this.backupMasterAddressZnode = ZKUtil.joinZNode(baseZNode,
				conf.get("zookeeper.znode.backupmaster", "backup-masters"));
	}

	@Override
	public void nodeCreated(String path) {
		if (path.equals(masterAddressZNode) && !master.isStopped()) {
			handleMasterNodeChange();
		}
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.equals(masterAddressZNode) && !master.isStopped()) {
			handleMasterNodeChange();
		}
	}

	/**
	 * Handle a change in the master node. Doesn't matter whether this was
	 * called from a nodeCreated or nodeDeleted event because there are no
	 * guarantees that the current state of the master node matches the event at
	 * the time of our next ZK request.
	 * 
	 * <p>
	 * Uses the watchAndCheckExists method which watches the master address node
	 * regardless of whether it exists or not. If it does exist (there is an
	 * active master), it returns true. Otherwise it returns false.
	 * 
	 * <p>
	 * A watcher is set which guarantees that this method will get called again
	 * if there is another change in the master node.
	 */
	private void handleMasterNodeChange() {
		// Watch the node and check if it exists.
		try {
			synchronized (clusterHasActiveMaster) {
				if (ZKUtil.watchAndCheckExists(watcher, masterAddressZNode)) {
					// A master node exists, there is an active master
					LOG.debug("A master is now available");
					clusterHasActiveMaster.set(true);
				} else {
					// Node is no longer there, cluster does not have an active
					// master
					LOG.debug("No master available. Notifying waiting threads");
					clusterHasActiveMaster.set(false);
					// Notify any thread waiting to become the active master
					clusterHasActiveMaster.notifyAll();
				}
			}
		} catch (KeeperException ke) {
			master.abort("Received an unexpected KeeperException, aborting", ke);
		}
	}

	/**
	 * Block until becoming the active master.
	 * 
	 * Method blocks until there is not another active master and our attempt to
	 * become the new active master is successful.
	 * 
	 * This also makes sure that we are watching the master znode so will be
	 * notified if another master dies.
	 * 
	 * @param startupStatus
	 * @return True if no issue becoming active master else false if another
	 *         master was running or if some other problem (zookeeper, stop flag
	 *         has been set on this Master)
	 */
	boolean blockUntilBecomingActiveMaster() {
		boolean cleanSetOfActiveMaster = true;
		// Try to become the active master, watch if there is another master.
		// Write out our ServerName as versioned bytes.
		try {
			String backupZNode = ZKUtil.joinZNode(backupMasterAddressZnode, master.getServerName());
			if (ZKUtil.createEphemeralNodeAndWatch(this.watcher, this.masterAddressZNode, master.getServerName()
					.getBytes())) {
				// If we were a backup master before, delete our ZNode from the
				// backup
				// master directory since we are the active now
				LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory");
				ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode);

				// We are the master, exec startPrepare.sh, return
				this.clusterHasActiveMaster.set(true);
				this.startPrepare();
				LOG.info("Master=" + master.getServerName());
				return cleanSetOfActiveMaster;
			}
			cleanSetOfActiveMaster = false;

			// There is another active master running elsewhere or this is a
			// restart
			// and the master ephemeral node has not expired yet.
			this.clusterHasActiveMaster.set(true);

			/*
			 * Add a ZNode for ourselves in the backup master directory since we
			 * are not the active master.
			 * 
			 * If we become the active master later, ActiveMasterManager will
			 * delete this node explicitly. If we crash before then, ZooKeeper
			 * will delete this node for us since it is ephemeral.
			 */
			LOG.info("Adding ZNode for " + backupZNode + " in backup master directory");
			ZKUtil.createEphemeralNodeAndWatch(this.watcher, backupZNode, master.getServerName().getBytes());

			String msg;
			byte[] bytes = ZKUtil.getDataAndWatch(this.watcher, this.masterAddressZNode);
			if (bytes == null) {
				msg = ("A master was detected, but went down before its address "
						+ "could be read.  Attempting to become the next active master");
			} else {
				String currentMaster = new String(bytes);
				if (currentMaster.equals(this.master.getServerName())) {
					msg = ("Current master has this master's address, " + currentMaster
							+ "; master was restarted?  Waiting on znode " + "to expire...");
					// Hurry along the expiration of the znode.
					ZKUtil.deleteNode(this.watcher, this.masterAddressZNode);
				} else {
					msg = "Another master is the active master, " + currentMaster
							+ "; waiting to become the next active master";
				}
			}
			LOG.info(msg);
		} catch (KeeperException ke) {
			master.abort("Received an unexpected KeeperException, aborting", ke);
			return false;
		}
		synchronized (this.clusterHasActiveMaster) {
			while (this.clusterHasActiveMaster.get() && !this.master.isStopped()) {
				try {
					this.clusterHasActiveMaster.wait();
				} catch (InterruptedException e) {
					// We expect to be interrupted when a master dies, will fall
					// out if so
					LOG.debug("Interrupted waiting for master to die", e);
				}
			}
			if (this.master.isStopped()) {
				return cleanSetOfActiveMaster;
			}
			// Try to become active master again now that there is no active
			// master
			blockUntilBecomingActiveMaster();
		}
		return cleanSetOfActiveMaster;
	}

	private void startPrepare() {
		String shellPath = ActiveMasterManager.class.getClassLoader().getResource("").getPath().replace("classes/", "");
		DefaultExecutor executor = new DefaultExecutor();
		CommandLine cmd = new CommandLine(shellPath + "prepare.sh");
		try {
			executor.execute(cmd);
		} catch (ExecuteException e) {
			LOG.debug("prepare.sh exec faild \n"+e);
		} catch (IOException e) {
			LOG.debug("prepare.sh exec faild \n"+e);
		}

	}

	/**
	 * @return True if cluster has an active master.
	 */
	public boolean isActiveMaster() {
		try {
			if (ZKUtil.checkExists(watcher, masterAddressZNode) >= 0) {
				return true;
			}
		} catch (KeeperException ke) {
			LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
		}
		return false;
	}

	public void stop() {
		try {
			// If our address is in ZK, delete it on our way out
			byte[] bytes = ZKUtil.getDataAndWatch(watcher, masterAddressZNode);
			if (bytes != null && new String(bytes).equals(master.getServerName())) {
				ZKUtil.deleteNode(watcher, masterAddressZNode);
			}
		} catch (KeeperException e) {
			LOG.error(this.watcher.prefix("Error deleting our own master address node"), e);
		}
	}
}
