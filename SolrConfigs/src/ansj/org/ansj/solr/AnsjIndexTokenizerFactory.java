package org.ansj.solr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.AttributeSource.AttributeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mlcs.search.mlcsseg.lucene.ReloadableTokenizerFactory;
import com.mlcs.search.mlcsseg.lucene.ZKTokenizerReloaderRegister;

public class AnsjIndexTokenizerFactory extends ReloadableTokenizerFactory
{

	private final Logger	LOG				= LoggerFactory
													.getLogger(AnsjIndexTokenizerFactory.class);

	private final int		analysisType	= 0;
	private boolean			rmPunc			= true;
	private String			configForder;
	private final String	dicName			= "dic.properties";									// 词库读取配置文件
	private final String	ambiguity		= "ambiguity.dic";										// 歧义词配置文件
	private final String	customer		= "consumer.properties";								// 歧义词配置文件

	public AnsjIndexTokenizerFactory(Map<String, String> args)
	{
		super(args);
		rmPunc = getBoolean(args, "rmPunc", true);
		configForder = get(args, "config");
		if (!configForder.endsWith(File.separator))
		{
			configForder += File.separator;
		}
		LOG.info(":::配置文件目录::::::::::::::::::::::::::" + configForder);
	}

	@Override
	public void inform(ResourceLoader loader) throws IOException
	{
		// ZKTokenizerReloaderRegister.registerCustomeDic(cdic); // 添加监听
		// 添加监听歧义词处理
		ZKTokenizerReloaderRegister.registerAmbiguityDic(configForder
				+ ambiguity);
		loadDic(configForder + dicName); // 加载词库
		addDicListener(configForder + customer); // 注册监听新词动态添加
	}

	@Override
	public Tokenizer create(AttributeFactory factory, Reader input)
	{
		return new AnsjTokenizer(input, analysisType, rmPunc);
	}

	@Override
	public void update(List<InputStream> inputStreams)
	{
	}
}
