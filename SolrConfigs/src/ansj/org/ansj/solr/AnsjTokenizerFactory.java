package org.ansj.solr;

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

public class AnsjTokenizerFactory extends ReloadableTokenizerFactory
{

	private final Logger	LOG				= LoggerFactory
													.getLogger(AnsjTokenizerFactory.class);

	private int				analysisType	= 0;
	private boolean			rmPunc			= true;
	private final String	cdic;
	private final String	adic;

	public AnsjTokenizerFactory(Map<String, String> args)
	{
		super(args);
		analysisType = getInt(args, "analysisType", 0);
		rmPunc = getBoolean(args, "rmPunc", true);
		cdic = get(args, "cdic");
		adic = get(args, "adic");
		LOG.info(":::ansj:用户自定义词典::::::::::::::::::::::::::" + cdic);
		LOG.info(":::ansj:歧义辞典::::::::::::::::::::::::::" + adic);
	}

	@Override
	public void inform(ResourceLoader loader) throws IOException
	{
		ZKTokenizerReloaderRegister.registerCustomeDic(cdic); // 添加监听
		ZKTokenizerReloaderRegister.registerAmbiguityDic(adic);
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
