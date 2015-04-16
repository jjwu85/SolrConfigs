package org.apache.lucene.analysis.synonym;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class DBSlowSynonymFilterFactory extends TokenFilterFactory implements
		ResourceLoaderAware
{

	private final Logger				LOG		= LoggerFactory
														.getLogger(DBSlowSynonymFilterFactory.class);

	private final String				synonyms;
	private final boolean				ignoreCase;
	private final boolean				expand;
	private final String				tf;
	private final Map<String, String>	tokArgs	= new HashMap<String, String>();

	public DBSlowSynonymFilterFactory(Map<String, String> args)
	{
		super(args);
		synonyms = require(args, "synonyms");
		ignoreCase = getBoolean(args, "ignoreCase", false);
		expand = getBoolean(args, "expand", true);

		tf = get(args, "tokenizerFactory");
		if (tf != null)
		{
			assureMatchVersion();
			tokArgs.put("luceneMatchVersion", getLuceneMatchVersion()
					.toString());
			for (Iterator<String> itr = args.keySet().iterator(); itr.hasNext();)
			{
				String key = itr.next();
				tokArgs.put(key.replaceAll("^tokenizerFactory\\.", ""),
						args.get(key));
				itr.remove();
			}
		}
		if (!args.isEmpty())
		{
			throw new IllegalArgumentException("Unknown parameters: " + args);
		}
	}

	@Override
	public void inform(ResourceLoader loader) throws IOException
	{
		TokenizerFactory tokFactory = null;
		if (tf != null)
		{
			tokFactory = loadTokenizerFactory(loader, tf);
		}

		Iterable<String> wlist = loadRules(synonyms, loader);

		synMap = new SlowSynonymMap(ignoreCase);
		parseRules(wlist, synMap, "=>", ",", expand, tokFactory);
	}

	/**
	 * @return a list of all rules
	 */
	protected Iterable<String> loadRules(String synonyms, ResourceLoader loader)
			throws IOException
	{

		Map<String, String> config = getConnectionConfig(synonyms, loader);
		LOG.info("同义词 driver ：" + config.get("driver"));
		LOG.info("同义词 url ：" + config.get("url"));
		LOG.info("同义词 SQL：" + config.get("sql"));

		Map<String, String> result = new HashMap<String, String>();
		Connection conn = null;
		try
		{
			Class.forName(config.get("driver")); // Oracle驱动加载
			conn = DriverManager.getConnection(config.get("url"),
					config.get("username"), config.get("password")); // 创建连接对象
			if (null == conn)
			{
				return Lists.newArrayList();
			}
			conn.setAutoCommit(true);
			String sql = config.get("sql");

			PreparedStatement stmt = conn.prepareStatement(sql);

			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next())
			{
				String name = rs.getString("NAME");
				String groupId = rs.getString("GROUPID");
				String values = result.get(groupId);
				if (null == values)
				{
					values = name;
				}
				else
				{
					values = values + "," + name;
				}
				result.put(groupId, values);
			}
			LOG.info("同义词获取成功。一共获取了{}组同义词", result.size());
		}
		catch (SQLException e)
		{
			LOG.error("获取数据发生异常", e);
		}
		catch (ClassNotFoundException e)
		{
			LOG.error("无法获取数据库驱动。", e);
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
		return result.values();
	}

	private Map<String, String> getConnectionConfig(String synonyms,
			ResourceLoader loader) throws IOException
	{
		List<String> wlist = null;
		File synonymFile = new File(synonyms);
		if (synonymFile.exists())
		{
			wlist = getLines(loader, synonyms);
		}
		else
		{
			List<String> files = splitFileNames(synonyms);
			wlist = new ArrayList<String>();
			for (String file : files)
			{
				List<String> lines = getLines(loader, file.trim());
				wlist.addAll(lines);
			}
		}
		Map<String, String> result = new HashMap<String, String>(wlist.size());
		if (!wlist.isEmpty())
		{ // 读取配置文件内容
			for (String line : wlist)
			{
				if (StringUtils.isNotBlank(line))
				{
					String[] entry = line.split("=", 2);
					result.put(entry[0], entry[1]);
				}
			}
		}
		if (result.isEmpty())
		{ // add default
			LOG.info("获取synonymsDBconfig配置文件内容为空，启用默认数据库配置。");
			result.put("url", "jdbc:oracle:thin:@10.1.0.207:1521:ORCL");
			result.put("username", "KMSEARCH");
			result.put("password", "KMSEARCH");
			result.put("driver", "oracle.jdbc.driver.OracleDriver");
		}
		return result;
	}

	private SlowSynonymMap	synMap;

	static void parseRules(Iterable<String> rules, SlowSynonymMap map,
			String mappingSep, String synSep, boolean expansion,
			TokenizerFactory tokFactory) throws IOException
	{
		for (String rule : rules)
		{
			// To use regexes, we need an expression that specifies an odd
			// number of chars.
			// This can't really be done with string.split(), and since we need
			// to
			// do unescaping at some point anyway, we wouldn't be saving any
			// effort
			// by using regexes.

			List<String> mapping = splitSmart(rule, mappingSep, false);

			List<List<String>> source;
			List<List<String>> target;

			if (mapping.size() > 2)
			{
				throw new IllegalArgumentException("Invalid Synonym Rule:"
						+ rule);
			}
			else if (mapping.size() == 2)
			{
				source = getSynList(mapping.get(0), synSep, tokFactory);
				target = getSynList(mapping.get(1), synSep, tokFactory);
			}
			else
			{
				source = getSynList(mapping.get(0), synSep, tokFactory);
				if (expansion)
				{
					// expand to all arguments
					target = source;
				}
				else
				{
					// reduce to first argument
					target = new ArrayList<List<String>>(1);
					target.add(source.get(0));
				}
			}

			boolean includeOrig = false;
			for (List<String> fromToks : source)
			{
				for (List<String> toToks : target)
				{
					map.add(fromToks, SlowSynonymMap.makeTokens(toToks),
							includeOrig, true);
				}
			}
		}
	}

	// a , b c , d e f => [[a],[b,c],[d,e,f]]
	private static List<List<String>> getSynList(String str, String separator,
			TokenizerFactory tokFactory) throws IOException
	{
		List<String> strList = splitSmart(str, separator, false);
		// now split on whitespace to get a list of token strings
		List<List<String>> synList = new ArrayList<List<String>>();
		for (String toks : strList)
		{
			List<String> tokList = tokFactory == null ? splitWS(toks, true)
					: splitByTokenizer(toks, tokFactory);
			synList.add(tokList);
		}
		return synList;
	}

	private static List<String> splitByTokenizer(String source,
			TokenizerFactory tokFactory) throws IOException
	{
		StringReader reader = new StringReader(source);
		TokenStream ts = loadTokenizer(tokFactory, reader);
		List<String> tokList = new ArrayList<String>();
		try
		{
			CharTermAttribute termAtt = ts
					.addAttribute(CharTermAttribute.class);
			while (ts.incrementToken())
			{
				if (termAtt.length() > 0) tokList.add(termAtt.toString());
			}
		}
		finally
		{
			reader.close();
		}
		return tokList;
	}

	private TokenizerFactory loadTokenizerFactory(ResourceLoader loader,
			String cname) throws IOException
	{
		Class<? extends TokenizerFactory> clazz = loader.findClass(cname,
				TokenizerFactory.class);
		try
		{
			TokenizerFactory tokFactory = clazz.getConstructor(Map.class)
					.newInstance(tokArgs);
			if (tokFactory instanceof ResourceLoaderAware)
			{
				((ResourceLoaderAware) tokFactory).inform(loader);
			}
			return tokFactory;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	private static TokenStream loadTokenizer(TokenizerFactory tokFactory,
			Reader reader)
	{
		return tokFactory.create(reader);
	}

	public SlowSynonymMap getSynonymMap()
	{
		return synMap;
	}

	@Override
	public SlowSynonymFilter create(TokenStream input)
	{
		return new SlowSynonymFilter(input, synMap);
	}

	public static List<String> splitWS(String s, boolean decode)
	{
		ArrayList<String> lst = new ArrayList<String>(2);
		StringBuilder sb = new StringBuilder();
		int pos = 0, end = s.length();
		while (pos < end)
		{
			char ch = s.charAt(pos++);
			if (Character.isWhitespace(ch))
			{
				if (sb.length() > 0)
				{
					lst.add(sb.toString());
					sb = new StringBuilder();
				}
				continue;
			}

			if (ch == '\\')
			{
				if (!decode) sb.append(ch);
				if (pos >= end) break; // ERROR, or let it go?
				ch = s.charAt(pos++);
				if (decode)
				{
					switch (ch) {
					case 'n':
						ch = '\n';
						break;
					case 't':
						ch = '\t';
						break;
					case 'r':
						ch = '\r';
						break;
					case 'b':
						ch = '\b';
						break;
					case 'f':
						ch = '\f';
						break;
					}
				}
			}

			sb.append(ch);
		}

		if (sb.length() > 0)
		{
			lst.add(sb.toString());
		}

		return lst;
	}

	/**
	 * Splits a backslash escaped string on the separator.
	 * <p>
	 * Current backslash escaping supported: <br>
	 * \n \t \r \b \f are escaped the same as a Java String <br>
	 * Other characters following a backslash are produced verbatim (\c => c)
	 * 
	 * @param s
	 *            the string to split
	 * @param separator
	 *            the separator to split on
	 * @param decode
	 *            decode backslash escaping
	 */
	public static List<String> splitSmart(String s, String separator,
			boolean decode)
	{
		ArrayList<String> lst = new ArrayList<String>(2);
		StringBuilder sb = new StringBuilder();
		int pos = 0, end = s.length();
		while (pos < end)
		{
			if (s.startsWith(separator, pos))
			{
				if (sb.length() > 0)
				{
					lst.add(sb.toString());
					sb = new StringBuilder();
				}
				pos += separator.length();
				continue;
			}

			char ch = s.charAt(pos++);
			if (ch == '\\')
			{
				if (!decode) sb.append(ch);
				if (pos >= end) break; // ERROR, or let it go?
				ch = s.charAt(pos++);
				if (decode)
				{
					switch (ch) {
					case 'n':
						ch = '\n';
						break;
					case 't':
						ch = '\t';
						break;
					case 'r':
						ch = '\r';
						break;
					case 'b':
						ch = '\b';
						break;
					case 'f':
						ch = '\f';
						break;
					}
				}
			}

			sb.append(ch);
		}

		if (sb.length() > 0)
		{
			lst.add(sb.toString());
		}

		return lst;
	}
}