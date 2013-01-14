/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sohu.dc.jobkeeper.helper;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class NodeManager extends ProtocolSupport {

	private static final Log LOG = LogFactory.getLog(NodeManager.class);
	private final String dir;

	@Override
	public void doClose() {
		try {
			this.zookeeper.close();
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
	}

	/**
	 * zookeeper contructor
	 * 
	 * @param zookeeper
	 *            zookeeper client instance
	 * @param dir
	 *            the parent path you want to use for locking
	 * @param acls
	 *            the acls that you want to use for all the paths, if null world
	 *            read/write is used.
	 */
	public NodeManager(ZooKeeper zookeeper, String dir, List<ACL> acl) {
		super(zookeeper);
		this.dir = dir;
		if (acl != null) {
			setAcl(acl);
		}
	}

	/**
	 * Attempts to acquire the exclusive write lock returning whether or not it
	 * was acquired. Note that the exclusive lock may be acquired some time
	 * later after this method has been invoked due to the current lock owner
	 * going away.
	 */
	public synchronized void createNode(CreateMode createMode) {
		if (isClosed()) {
			return;
		}
		ensurePathExists(dir, createMode);
	}
	
	public synchronized void createPath(CreateMode createMode){
		if(isClosed()){
			return;
		}
		String[] dirs = dir.split("/");
		for(int i=0;i<dirs.length-1;i++){
			String path = StringUtils.join(Arrays.copyOfRange(dirs, 0, i+2),"/");
			ensurePathExists(path, createMode);
		}
	}

	/**
	 * Attempts to acquire the exclusive write lock returning whether or not it
	 * was acquired. Note that the exclusive lock may be acquired some time
	 * later after this method has been invoked due to the current lock owner
	 * going away.
	 */
	public synchronized void createNode(CreateMode createMode, byte[] data) {
		if (isClosed()) {
			return;
		}
		ensurePathExists(dir, data, createMode);

	}

	public List<String> getNode(Watcher watcher) {
		try {
			List<String> names = zookeeper.getChildren(dir, watcher);
			return names;
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
		return null;
	}

	public synchronized String getData(Watcher watcher) {
		try {
			byte[] args = zookeeper.getData(dir, watcher, null);
			return new String(args);
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
		return null;
	}

	public synchronized void setData() {
		try {
			Stat stat = zookeeper.exists(dir, null);
			String str = String.valueOf(stat.getVersion() + 1);
			zookeeper.setData(dir, str.getBytes(), stat.getVersion());
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
	}

	public synchronized void setData(byte[] data) {
		try {
			Stat stat = zookeeper.exists(dir, false);
			zookeeper.setData(dir, data, stat.getVersion());
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
	}

	public synchronized void close() {
		try {
			zookeeper.close();
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
	}

	public synchronized boolean ensureExists(Watcher watcher) {
		Stat stat;
		try {
			stat = zookeeper.exists(dir, watcher);
			if (stat != null) {
				return true;
			}
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
		return false;
	}

	private List<String> getNode(String parentPath) {
		try {
			List<String> names = zookeeper.getChildren(parentPath, false);
			return names;
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
		return null;
	}

	private void deleteNode(String path) {
		try {
			zookeeper.delete(path, -1);
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
	}

	private void deletePath(String parent) {
		List<String> children = getNode(parent);
		if (children != null) {
			for (String child : children) {
				deletePath(parent.equals("/") ? "/" + child : parent + "/"
						+ child);
			}
		}
		deleteNode(parent);
	}

	public synchronized void deleteNode() {
		this.deletePath(dir);
	}

	public synchronized boolean ensureExists(boolean watcher) {
		Stat stat;
		try {
			stat = zookeeper.exists(dir, watcher);
			if (stat != null) {
				return true;
			}
		} catch (KeeperException e) {
			LOG.warn("Caught Exception", e);
		} catch (InterruptedException e) {
			LOG.warn("Caught Exception", e);
		}
		return false;
	}

	/**
	 * return the parent dir for lock
	 * 
	 * @return the parent dir used for locks.
	 */
	public String getDir() {
		return dir;
	}
}
