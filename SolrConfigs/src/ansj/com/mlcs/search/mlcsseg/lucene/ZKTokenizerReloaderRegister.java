package com.mlcs.search.mlcsseg.lucene;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.zk.ZK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlcs.search.mlcsseg.common.ANSJDicLoderListener;
import com.mlcs.search.mlcsseg.common.AmbiguityDicLoderListener;

/**
 * register it in 'inform(ResourceLoader loader)'
 * 
 * @Description TODO
 * @author shanbo.liang
 */
public class ZKTokenizerReloaderRegister
{

	private static final Logger					LOG				= LoggerFactory
																		.getLogger(ZKTokenizerReloaderRegister.class);

	private static Map<String, ConfigChecker>	reloadAwares	= new HashMap<String, ConfigChecker>();

	public static class ConfigChecker
	{

		public static List<String> SplitFileNames(String fileNames)
		{
			if (fileNames == null || fileNames.isEmpty()) return Collections
					.emptyList();

			List<String> result = new ArrayList<String>();
			for (String file : fileNames.split("[,\\s]+"))
			{
				result.add(file);
			}

			return result;
		}

		public List<String> currentToReload(String confName)
		{
			try
			{
				List<String> dicPaths = SplitFileNames(confName);
				return dicPaths;
			}
			catch (Exception e)
			{
				return Collections.emptyList();
			}
		}

	}

	/**
	 * 向注册机注册一个可定时更新的tokenfactory；register it in 'inform(ResourceLoader loader)'
	 * 
	 * @param reloadFactory
	 * @param loader
	 * @param confName
	 * @return
	 */
	public static synchronized String registerCustomeDic(final String confName)
	{
		if (reloadAwares.containsKey(confName))
		{
			return "already";
		}
		else
		{
			if (confName != null && !confName.trim().isEmpty())
			{ // 存在conf才注册进来
				final ConfigChecker cc = new ConfigChecker();
				reloadAwares.put(confName, cc);
				addCustomeDicListen(cc, confName);
			}
			return "conf is empty";
		}
	}

	private static void addCustomeDicListen(final ConfigChecker cc,
			final String confName)
	{

		try
		{
			List<String> dicts = cc.currentToReload(confName);
			if (!dicts.isEmpty())
			{
				for (String dicPath : dicts)
				{
					if (null == dicPath || dicPath.trim().isEmpty()) continue;
					ANSJDicLoderListener configListener = new ANSJDicLoderListener(
							dicPath);
					configListener.executor(ZK.me().getClient()); // 启动DIC监听
					ZK.me().registerListener(configListener); // 加入ZK重连监听
				}
			}
		}
		catch (Exception e)
		{
			LOG.error("loadAndUpdate get exception: ", e);
		}
	}

	public static synchronized String registerAmbiguityDic(final String confName)
	{
		if (reloadAwares.containsKey(confName))
		{
			return "already";
		}
		else
		{
			if (confName != null && !confName.trim().isEmpty())
			{ // 存在conf才注册进来
				final ConfigChecker cc = new ConfigChecker();
				reloadAwares.put(confName, cc);
				addAmbiguityListen(cc, confName);
			}
			return "conf is empty";
		}
	}

	private static void addAmbiguityListen(final ConfigChecker cc,
			final String confName)
	{

		try
		{
			List<String> dicts = cc.currentToReload(confName);
			if (!dicts.isEmpty())
			{
				for (String dicPath : dicts)
				{
					if (null == dicPath || dicPath.trim().isEmpty()) continue;
					AmbiguityDicLoderListener configListener = new AmbiguityDicLoderListener(
							dicPath);
					configListener.executor(ZK.me().getClient()); // 启动DIC监听
					ZK.me().registerListener(configListener); // 加入ZK重连监听
				}
			}
		}
		catch (Exception e)
		{
			LOG.error("loadAndUpdate get exception: ", e);
		}
	}
}
