package org.apache.lucene.analysis.synonym;

import java.io.ByteArrayInputStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.zk.ZKStartListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 系统启动后从ZK集群中加载搜索相关配置
 * 
 * @author river
 * 
 */
public class SynonymLoderListener implements ZKStartListener
{

	private final Logger		LOG	= LoggerFactory
											.getLogger(SynonymLoderListener.class);

	String						nodePath;

	ZKSlowSynonymFilterFactory	factory;

	ResourceLoader				loader;

	public SynonymLoderListener(String nodePath,
			ZKSlowSynonymFilterFactory factory, ResourceLoader loader)
	{
		this.nodePath = nodePath;
		this.factory = factory;
		this.loader = loader;
	}

	@Override
	public void executor(CuratorFramework client)
	{
		final ZKSlowSynonymFilterFactory factory = this.factory;
		final String nodePath = this.nodePath;
		final NodeCache cache = new NodeCache(client, nodePath);

		cache.getListenable().addListener(new NodeCacheListener()
		{
			@Override
			public void nodeChanged() throws Exception
			{
				byte[] data = cache.getCurrentData().getData();
				if (null != data)
				{
					try
					{
						ByteArrayInputStream in = new ByteArrayInputStream(
								data, 0, data.length);
						factory.update(in, loader);
						LOG.info("NodeCache reload synonym for path: {}",
								nodePath);
					}
					catch (Exception e)
					{
						LOG.error("解析ZK node:{}集群数据,加载配置文件失败!", nodePath, e);
					}
				}
				else
				{
					LOG.info(
							"Not found config from ZK. node path:{}. will load config from search.properties",
							nodePath);
				}
			}
		});
		try
		{
			cache.start(false);
			LOG.info(">>>>>>  Start NodeCache for path: {}", nodePath);
		}
		catch (Exception e)
		{
			LOG.error("Start NodeCache error for path: {}, error info: {}",
					nodePath, e.getMessage());
		}
	}
}
