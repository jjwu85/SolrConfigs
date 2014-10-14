package org.apache.solr.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZK
{

	private CuratorFramework	client;

	private static final ZK		zk		= new ZK();

	private final Logger		logger	= LoggerFactory.getLogger(ZK.class);

	private ZK()
	{
		init();
	}

	public static ZK me()
	{
		return zk;
	}

	public CuratorFramework getClient()
	{
		return client;
	}

	// 创建ZK链接
	private void init()
	{
		// host:poit/solr
		String zkHost = System.getProperty("zkHost");
		// 1000 是重试间隔时间基数，3 是重试次数
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = createWithOptions(zkHost, retryPolicy, 10000, 10000);
		client.start();
	}

	/**
	 * 通过自定义参数创建
	 */
	private CuratorFramework createWithOptions(String connectionString,
			RetryPolicy retryPolicy, int connectionTimeoutMs,
			int sessionTimeoutMs)
	{
		return CuratorFrameworkFactory.builder()
				.connectString(connectionString).retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs)
				.sessionTimeoutMs(sessionTimeoutMs).build();
	}

	// 注册需要监听的监听者对像.
	public void registerListener(final ZKStartListener listener)
	{
		client.getConnectionStateListenable().addListener(
				new ConnectionStateListener()
				{
					@Override
					public void stateChanged(CuratorFramework client,
							ConnectionState newState)
					{
						logger.info("CuratorFramework state changed: {}",
								newState);
						if (newState == ConnectionState.CONNECTED
								|| newState == ConnectionState.RECONNECTED)
						{
							listener.executor(client);
							logger.info("Listener {} executed!", listener
									.getClass().getName());
						}
					}
				});

		client.getUnhandledErrorListenable().addListener(
				new UnhandledErrorListener()
				{
					@Override
					public void unhandledError(String message, Throwable e)
					{
						logger.info("CuratorFramework unhandledError: {}",
								message);
					}
				});
	}
}
