package org.apache.solr.handler.component;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.zk.ZKStartListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 系统启动后从ZK集群中加载搜索相关配置
 * 
 * @author river
 * 
 */
public class ElevationConfigLoderListener implements ZKStartListener
{

	private final Logger		LOG	= LoggerFactory
											.getLogger(ElevationConfigLoderListener.class);

	String						nodePath;

	ZKQueryElevationComponent	qec;

	SolrCore					core;

	public ElevationConfigLoderListener(ZKQueryElevationComponent qec,
			String nodePath, SolrCore core)
	{
		this.nodePath = nodePath;
		this.qec = qec;
		this.core = core;
	}

	@Override
	public void executor(CuratorFramework client)
	{
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
						qec.loadElevation(core);
						LOG.info("重新加载Elevation配置成功! node path:{}", nodePath);
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
