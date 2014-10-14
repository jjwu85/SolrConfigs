package org.apache.solr.zk;

import org.apache.curator.framework.CuratorFramework;

/**
 * ZK集群启动监听接口
 * 
 * @author river
 * 
 */
public interface ZKStartListener
{
	void executor(CuratorFramework client);
}
