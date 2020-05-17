package com.kafka.algo.runners.utils;

import static com.kafka.algo.runners.constants.Constants.ZNODE_PREFIX;
import static com.kafka.algo.runners.constants.Constants.ZNODE_START;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.kafkautils.KafkaConnection;;

/**
 * @author justin
 *
 */
public class ZkConnect {
	private static final Logger LOGGER = Logger.getLogger(ZkConnect.class.getName());
	private ZooKeeper zk;
	private CountDownLatch connSignal = new CountDownLatch(0);
	private String znodeName;
	private KafkaConfigReader configReader;
	@SuppressWarnings("unused")
	private boolean reset;

	/**
	 * @param topicName
	 * @param reset
	 * @param configReader
	 */
	public ZkConnect(final String topicName, final boolean reset, final KafkaConfigReader configReader) {
		this.configReader = configReader;
		this.znodeName = ZNODE_PREFIX + ZNODE_START + configReader.getAppVersion() + "-" + topicName;
		this.zk = this.connect(configReader.getZookeeperHost());
		this.reset = reset;
		createNode(0L, topicName);
	}

	/**
	 * Create Zookeeper client object to get apis
	 * 
	 * @param host
	 * @return
	 */
	private ZooKeeper connect(final String host) {
		try {
			zk = new ZooKeeper(host, 3000, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == KeeperState.SyncConnected) {
						connSignal.countDown();
					}
				}
			});
			connSignal.await();
		} catch (IOException | InterruptedException e) {
			LOGGER.error("Exception while getting connection of the node.. " + e.getMessage());
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
	 * @param <K>
	 * @param <V>
	 * @param data
	 * @param topicName
	 */
	private <K, V> void createNode(final long data, final String topicName) {
		final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
				KafkaConnection.getKafkaProducerProperties(this.configReader));
		producer.partitionsFor(topicName).forEach(partitionInfo -> {
			final int partitionNumber = partitionInfo.partition();
			try {
				final String znodeUpdated = this.znodeName + "-" + partitionNumber;
				final Stat nodeExistence = zk.exists(znodeUpdated, true);
				if (nodeExistence != null) {
					if (this.reset = true) {
						zk.delete(znodeUpdated, zk.exists(znodeUpdated, true).getVersion());
						zk.create(znodeUpdated, String.valueOf(data).getBytes(), Ids.OPEN_ACL_UNSAFE,
								CreateMode.PERSISTENT);
					} else {
						LOGGER.warn("Node Already Present. Do Nothing. Go For Update. Please Ignore If it is intended");
					}
				} else {
					zk.create(znodeUpdated, String.valueOf(data).getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
			} catch (KeeperException | InterruptedException e) {
				LOGGER.error("Exception while creating the node.. " + e.getMessage());
			}
		});
		producer.close();
	}

	/**
	 * @param data
	 * @throws Exception
	 */
	public void updateNode(final long data, final int partition) {
		final String znodeUpdated = this.znodeName + "-" + partition;
		try {
			zk.setData(znodeUpdated, String.valueOf(data).getBytes(), zk.exists(znodeUpdated, true).getVersion());
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("Exception while updating data in the zookeeper node.. " + e.getMessage());
		}
	}

	/**
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private void deleteNode(final int partitionNumber) {
		try {
			zk.delete(this.znodeName + "-" + partitionNumber,
					zk.exists(this.znodeName + "-" + partitionNumber, true).getVersion());
		} catch (InterruptedException | KeeperException e) {
			LOGGER.error("Exception while deletting one node.. " + e.getMessage());
		}
	}

	/**
	 * Deletes All the nodes corresponding to this application name
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private void deleteAllNode() {
		final List<String> zNodes;
		try {
			zNodes = zk.getChildren("/", true);
			for (String zNode : zNodes) {
				if (zNode.startsWith(ZNODE_START)) {
					System.out.println(zNode);
					zk.delete("/" + zNode, zk.exists("/" + zNode, true).getVersion());
				}
			}
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("Exception while delete all zookeeper node.. " + e.getMessage());
		}

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
	private byte[] getDataFromPath(final String zNode) throws KeeperException, InterruptedException {
		return zk.getData(zNode, true, zk.exists(zNode, true));
	}

	/**
	 * This is to find the minimum data in the each topic
	 * 
	 * @return minimumTimestamp
	 * @throws Exception
	 */
	public Long getMinimum() {
		final TreeSet<Long> treeSet = new TreeSet<Long>();
		final List<String> zNodes;
		try {
			zNodes = zk.getChildren("/", true);
			for (final String zNode : zNodes) {
				if (zNode.startsWith(ZNODE_START + this.configReader.getAppVersion())) {
					final String data = new String(getDataFromPath("/" + zNode));
					treeSet.add(Long.parseLong(data));
				}
			}
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("Exception while getting minimum value of the node.. " + e.getMessage());
		}

		return treeSet.isEmpty() ? 0L : treeSet.ceiling(100L);
	}

}