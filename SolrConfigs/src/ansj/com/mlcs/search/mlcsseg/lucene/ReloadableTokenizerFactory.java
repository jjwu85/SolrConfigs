package com.mlcs.search.mlcsseg.lucene;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.ansj.library.UserDefineLibrary;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.solr.zk.ZK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;

public abstract class ReloadableTokenizerFactory extends TokenizerFactory
		implements ResourceLoaderAware
{

	private final Logger	LOG	= LoggerFactory
										.getLogger(ReloadableTokenizerFactory.class);

	protected ReloadableTokenizerFactory(Map<String, String> args)
	{
		super(args);
		// assureMatchVersion();
	}

	public abstract void update(List<InputStream> inputStreams);

	public String getBeanName()
	{
		return this.getClass().toString();
	}

	protected void loadDic(String path)
	{
		Properties properties = null;
		try
		{
			byte[] data = getData(path);
			if (null == data)
			{
				return;
			}
			ByteArrayInputStream in = new ByteArrayInputStream(data, 0,
					data.length);
			properties = new Properties();
			properties.load(in);
		}
		catch (Exception e1)
		{
			LOG.error("读取{}配置信息失败！", path, e1);
		}
		if (null == properties)
		{
			return;
		}

		Connection conn = null;
		try
		{
			Class.forName(properties.get("driver").toString()); // Oracle驱动加载
			conn = DriverManager.getConnection(
					properties.get("url").toString(), properties
							.get("username").toString(),
					properties.get("password").toString()); // 创建连接对象
			if (null == conn)
			{
				LOG.warn("无法获取数据库连接，加载词库失败！");
				return;
			}
			conn.setAutoCommit(true);

			String countSql = properties.get("count.sql").toString();
			PreparedStatement countPS = conn.prepareStatement(countSql);
			ResultSet countRS = countPS.executeQuery();
			countRS.next();
			int count = countRS.getInt("count");
			LOG.info("用户自定义词库加载 一共需要加载{}个词", count);
			countRS.close();
			countPS.close();

			String querySql = properties.get("query.sql").toString();
			LOG.info("用户自定义词库加载 querySql:{}", querySql);

			PreparedStatement queryPS = conn.prepareStatement(querySql);
			int pageSize = 1000;
			int totalPage = count / pageSize + 1;
			int addCount = 0;
			for (int i = 0; i < totalPage; i++)
			{
				int p1 = (i + 1) * pageSize;
				int p2 = i * pageSize + 1;
				try
				{
					queryPS.setInt(1, p1);
					queryPS.setInt(2, p2);
					ResultSet queryRS = queryPS.executeQuery();
					while (queryRS.next())
					{
						String word = "";
						String nature = "";
						int freq = 1000;
						try
						{
							word = queryRS.getString("WORD");
							nature = queryRS.getString("NATURE");
							freq = queryRS.getInt("FREQ");
							UserDefineLibrary.insertWord(word, nature, freq);
							addCount++;
						}
						catch (Exception e)
						{
							LOG.error("word:{},nature:{},freq:{}",
									new Object[] { word, nature, freq }, e);
						}
					}
					queryRS.close();
				}
				catch (Exception e)
				{
					LOG.error("获取词库数据发生异常", e);
					LOG.error(querySql + "[p1]:" + p1 + "[p2]:" + p2);
					continue;
				}
			}
			LOG.info("用户自定义词库加载成功。一共加载了{}词", addCount);
		}
		catch (Exception e)
		{
			LOG.error("获取词库数据发生异常", e);
		}
		finally
		{
			try
			{
				if (null != conn)
				{
					conn.close();
				}
			}
			catch (SQLException e)
			{
			}
		}
	}

	protected void addDicListener(String path)
	{
		Properties properties = null;
		try
		{
			byte[] data = getData(path);
			if (null == data)
			{
				return;
			}
			ByteArrayInputStream in = new ByteArrayInputStream(data, 0,
					data.length);
			properties = new Properties();
			properties.load(in);
		}
		catch (Exception e1)
		{
			LOG.error("读取{}配置信息失败！", path, e1);
		}
		if (null == properties)
		{
			return;
		}
		// 集群环境中，所有机子上的消息监听器的GROUP.ID值都一样
		// 将导致只会其中一台机子接收到消息。
		// 简单做法是每个监听器GROUP.ID都不一样。这样监听器都接收到同一个topic消息
		final String ordinaryGid = properties.get("group.id").toString();
		String groupId = getHostName() + "." + ordinaryGid;
		properties.put("group.id", groupId);
		ConsumerConfig config = new ConsumerConfig(properties);

		final ConsumerConnector connector = Consumer
				.createJavaConsumerConnector(config);
		Map<String, Integer> topics = Maps.newHashMap();
		topics.put("customer-dic", 2);
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector
				.createMessageStreams(topics);
		List<KafkaStream<byte[], byte[]>> partitions = streams
				.get("customer-dic");
		for (final KafkaStream<byte[], byte[]> partition : partitions)
		{
			Executors.newFixedThreadPool(2).execute(new Runnable()
			{

				@Override
				public void run()
				{
					ConsumerIterator<byte[], byte[]> it = partition.iterator();
					while (it.hasNext())
					{
						try
						{
							MessageAndMetadata<byte[], byte[]> item = it.next();
							String message = new String(item.message(), "UTF-8");
							LOG.info("收到消息：{}", message);

							JSONObject root = JSON.parseObject(message);
							String channel = root.getString("channel");
							if (ordinaryGid.equalsIgnoreCase(channel))
							{
								String word = root.getString("word");
								String nature = root.getString("nature");
								int freq = root.getIntValue("freq");
								String action = root.getString("action");
								if ("add".equalsIgnoreCase(action))
								{
									UserDefineLibrary.insertWord(word, nature,
											freq);
									LOG.info("{} 成功添加到词库", word);
								}
								else
								{
									UserDefineLibrary.removeWord(word);
									LOG.info("{} 成功从词库中删除", word);
								}
							}
						}
						catch (Exception e)
						{
							LOG.error("接收处理消息失败！", e);
						}
					}
				}
			});
		}
		LOG.info("用户自定义词库加载监听器{} 启动成功！group id:{}", new String[] {
				"customer-dic", groupId });
	}

	protected byte[] getData(String path)
	{
		try
		{
			byte[] data = ZK.me().getClient().getData().forPath(path);
			if (null == data)
			{
				LOG.warn("读取{}配置信息内容为空！", path);
			}
			return data;
		}
		catch (Exception e)
		{
			LOG.warn("读取{}配置信息内容失败！", path);
		}
		return null;
	}

	protected String getHostNameForLiunx()
	{
		try
		{
			return (InetAddress.getLocalHost()).getHostName();
		}
		catch (UnknownHostException uhe)
		{
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null)
			{
				int colon = host.indexOf(':');
				if (colon > 0)
				{
					return host.substring(0, colon);
				}
			}
			return "UnknownHost";
		}
	}

	protected String getHostName()
	{
		if (System.getenv("COMPUTERNAME") != null)
		{
			return System.getenv("COMPUTERNAME");
		}
		else
		{
			return getHostNameForLiunx();
		}
	}
}
