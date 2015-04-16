package com.mlcs.search.mlcsseg.common;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import love.cq.domain.Forest;
import love.cq.library.Library;

import org.ansj.library.UserDefineLibrary;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.solr.zk.ZKStartListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 系统启动后从ZK集群中加载搜索相关配置
 * 
 * @author river
 * 
 */
public class ANSJDicLoderListener implements ZKStartListener
{

	private final Logger	LOG	= LoggerFactory
										.getLogger(ANSJDicLoderListener.class);

	private final String	nodePath;

	public ANSJDicLoderListener(String nodePath)
	{
		this.nodePath = nodePath;
	}

	@Override
	public void executor(CuratorFramework client)
	{
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
					ByteArrayInputStream in = new ByteArrayInputStream(data, 0,
							data.length);
					BufferedReader br = null;
					try
					{
						br = new BufferedReader(new InputStreamReader(in));
						String line = br.readLine();
						Forest forest = new Forest();
						while (StringUtils.isNotBlank(line))
						{
							try
							{
								Library.insertWord(forest, line);
							}
							catch (Exception e)
							{
								continue;
							}
							line = br.readLine();
						}
						// 将新构建的辞典树替换掉旧的。
						UserDefineLibrary.FOREST = forest;
					}
					catch (Exception e)
					{
						LOG.error("重新加载字典失败!", e);
					}
					finally
					{
						try
						{
							if (null != in) in.close();
							if (null != br) br.close();
						}
						catch (IOException e)
						{
						}
					}
					LOG.info("NodeCache reload dic for path: {}", nodePath);
				}
				else
				{
					LOG.info("Not found config from ZK. node path:{}.",
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
