package com.kafka.algo.runners.utils;

import static com.kafka.algo.runners.constants.Constants.ZNODE_PREFIX;
import static com.kafka.algo.runners.constants.Constants.ZNODE_START;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kafka.algo.runner.configreader.KafkaConfigReader;;

/**
 * @author justin
 *
 */
public class ZkConnect {
	private ZooKeeper zk;
	private CountDownLatch connSignal = new CountDownLatch(0);
	private String znodeName;
	private KafkaConfigReader configReader;

	/**
	 * @param topicName
	 * @param reset
	 * @param configReader
	 */
	public ZkConnect(final String topicName, final boolean reset, final KafkaConfigReader configReader) {
		this.configReader = configReader;
		this.znodeName = ZNODE_PREFIX + ZNODE_START + configReader.getAppVersion() + topicName;
		this.zk = this.connect(configReader.getZookeeperHost());
		createNode(0L, reset);
	}

	/**
	 * Create Zookeeper client object to get apis
	 * 
	 * @param host
	 * @return
	 */
	private ZooKeeper connect(String host) {
		try {
			zk = new ZooKeeper(host, 3000, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == KeeperState.SyncConnected) {
						connSignal.countDown();
					}
				}
			});
			connSignal.await();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return zk;
	}

	/**
	 * @throws InterruptedException
	 */
	public void close() throws InterruptedException {
		zk.close();
	}

	/**
	 * Create the node with default data in the initial stage.
	 * 
	 * @param path
	 * @param data
	 * @param reset
	 */
	private void createNode(long data, boolean reset) {
		// String znodeName = this.znodeName + "_" + partition;
		try {
			Stat nodeExistence = zk.exists(this.znodeName, true);

			if (nodeExistence != null) {
				if (reset = true) {
					zk.delete(this.znodeName, zk.exists(this.znodeName, true).getVersion());
					zk.create(this.znodeName, String.valueOf(data).getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				} else {
					System.out.println("Node Already Present. Do Nothing. Go For Update.");
				}
			} else {
				zk.create(this.znodeName, String.valueOf(data).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param data
	 * @throws Exception
	 */
	public void updateNode(long data, int partition) {
		// String znodeName = this.znodeName + "_" + partition;
		try {
			zk.setData(this.znodeName, String.valueOf(data).getBytes(), zk.exists(this.znodeName, true).getVersion());
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @throws Exception
	 */
	@Deprecated
	public void deleteNode() throws Exception {
		zk.delete(this.znodeName, zk.exists(this.znodeName, true).getVersion());
	}

	/**
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private byte[] getData() throws KeeperException, InterruptedException {
		return zk.getData(this.znodeName, true, zk.exists(this.znodeName, true));
	}

	/**
	 * @param zNode
	 * @return byte of data in the znode
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws Exception
	 */
	private byte[] getDataFromPath(String zNode) throws KeeperException, InterruptedException {
		return zk.getData(zNode, true, zk.exists(zNode, true));
	}

	/**
	 * This is to find the minimum data in the each topic
	 * 
	 * @return minimumTimestamp
	 * @throws Exception
	 */
	public Long getMinimum() {
		TreeSet<Long> treeSet = new TreeSet<Long>();
		List<String> zNodes;
		try {
			zNodes = zk.getChildren("/", true);
			for (String zNode : zNodes) {
				if (zNode.startsWith(ZNODE_START + this.configReader.getAppVersion())) {
					System.out.println(zNode);
					String data = new String(getDataFromPath("/" + zNode));
					treeSet.add(Long.parseLong(data));
				}
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}

		return treeSet.isEmpty() ? 0 : treeSet.ceiling(100L);
	}

}