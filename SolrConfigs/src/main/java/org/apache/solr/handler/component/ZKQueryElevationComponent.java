package org.apache.solr.handler.component;

import java.io.IOException;
import java.net.URL;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.zk.ZK;

/**
 * 动态加载Elevation配置 Elevation搜索组件
 * 
 * @author river
 * 
 */
public class ZKQueryElevationComponent extends QueryElevationComponent
{

	private SolrParams			initArgs		= null;

	public static final String	CONFIGS_ZKNODE	= "/configs";

	@Override
	public void init(NamedList args)
	{
		this.initArgs = SolrParams.toSolrParams(args);
		super.init(args);
	}

	@Override
	public void inform(SolrCore core)
	{
		ZkController zkController = core.getCoreDescriptor().getCoreContainer()
				.getZkController();
		try
		{
			// 获取配置文件在ZK中的路径
			String f = initArgs.get(CONFIG_FILE);
			if (f == null)
			{
				throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
						"QueryElevationComponent must specify argument: '"
								+ CONFIG_FILE + "' -- path to elevate.xml");
			}
			String collection = zkController.readConfigName(core
					.getCoreDescriptor().getCloudDescriptor()
					.getCollectionName());

			String path = CONFIGS_ZKNODE + "/" + collection + "/" + f;
			// 将配置文件所在节点添加到zk监听中,动态加载配置文件内容
			ElevationConfigLoderListener listener = new ElevationConfigLoderListener(
					this, path, core);
			listener.executor(ZK.me().getClient()); // 启动DIC监听
			ZK.me().registerListener(listener); // 加入ZK重连监听
		}
		catch (Exception e)
		{
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
					"Error initializing QueryElevationComponent.", e);
		}
		loadElevation(core);
	}

	void loadElevation(SolrCore core)
	{
		super.inform(core);
	}

	@Override
	public void prepare(ResponseBuilder rb) throws IOException
	{
		// TODO Auto-generated method stub
		super.prepare(rb);
	}

	@Override
	public void process(ResponseBuilder rb) throws IOException
	{
		// TODO Auto-generated method stub
		super.process(rb);
	}

	@Override
	public String getDescription()
	{
		// TODO Auto-generated method stub
		return super.getDescription();
	}

	@Override
	public String getSource()
	{
		// TODO Auto-generated method stub
		return super.getSource();
	}

	@Override
	public URL[] getDocs()
	{
		// TODO Auto-generated method stub
		return super.getDocs();
	}

}
