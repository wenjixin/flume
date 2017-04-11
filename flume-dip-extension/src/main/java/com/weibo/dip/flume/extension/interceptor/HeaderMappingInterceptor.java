/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weibo.dip.flume.extension.interceptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.common.base.Preconditions;

/**
 * 
 * Sample config:
 * <p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = com.weibo.dip.flume.extension.interceptor.HeaderMappingInterceptor$Builder<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = true<p>
 *   agent.sources.r1.interceptors.i1.header = header<p>
 *   agent.sources.r1.interceptors.i1.mapping = oldValue:newValue oldValue1:newValue2 oldValue3:newValue3<p>
 * </code>
 * 
 * @author tongwei
 * @date 2015年12月1日 下午5:05:40
 * @desc
 */
public class HeaderMappingInterceptor implements Interceptor {

	private final String header;
	private final Map<String, String> oldNewMapping;
	private final boolean preserveExisting;

	public HeaderMappingInterceptor(String header, String mapping, boolean preserveExisting) {
		super();
		this.preserveExisting = preserveExisting;
		this.header = header;
		this.oldNewMapping = new HashMap<String, String>();

		if (mapping != null && mapping.length() > 0) {
			String[] toknes = mapping.split(" ", -1);
			for (int i = 0; i < toknes.length; i++) {
				String[] keyValue = toknes[i].split(":", -1);
				oldNewMapping.put(keyValue[0], keyValue[1]);
			}
		}
	}

	@Override
	public void initialize() {
		// pass
	}

	@Override
	public Event intercept(Event event) {

		Map<String, String> headers = event.getHeaders();
		
		String headerValue = headers.get(header);
		
		if (headerValue != null) {
			String newValue = oldNewMapping.get(headerValue);
			if(newValue!=null){
				headers.put(header, newValue);
			}
		}
		
		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	@Override
	public void close() {
		// pass
	}

	public static class Builder implements Interceptor.Builder {

		private static final String HEADER = "header";
		private static final String MAPPING = "mapping";
		private static final String PRESERVE = "preserveExisting";
		private static final boolean PRESERVE_DFLT = false;

		private String header;
		private String oldNewMapping;
		private boolean preserveExisting = PRESERVE_DFLT;

		@Override
		public void configure(Context context) {
			header = context.getString(HEADER);
			oldNewMapping = context.getString(MAPPING);
			preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
		}

		@Override
		public Interceptor build() {
			Preconditions.checkNotNull(header, "From header required");
			Preconditions.checkNotNull(oldNewMapping, "TO header required");
			return new HeaderMappingInterceptor(header, oldNewMapping, preserveExisting);
		}

	}
}
