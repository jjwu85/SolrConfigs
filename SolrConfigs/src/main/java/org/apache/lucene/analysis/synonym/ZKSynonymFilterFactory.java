package org.apache.lucene.analysis.synonym;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

public class ZKSynonymFilterFactory extends TokenFilterFactory implements
		ResourceLoaderAware
{

	private final TokenFilterFactory	delegator;

	public ZKSynonymFilterFactory(Map<String, String> args)
	{
		super(args);
		assureMatchVersion();
		delegator = new ZKSlowSynonymFilterFactory(new HashMap<String, String>(
				getOriginalArgs()));
	}

	@Override
	public TokenStream create(TokenStream input)
	{
		return delegator.create(input);
	}

	@Override
	public void inform(ResourceLoader loader) throws IOException
	{
		((ResourceLoaderAware) delegator).inform(loader);
	}

	/**
	 * Access to the delegator TokenFilterFactory for test verification
	 * 
	 * @deprecated Method exists only for testing 4x, will be removed in 5.0
	 * @lucene.internal
	 */
	@Deprecated
	TokenFilterFactory getDelegator()
	{
		return delegator;
	}
}