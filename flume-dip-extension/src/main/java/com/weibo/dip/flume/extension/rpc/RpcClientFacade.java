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
package com.weibo.dip.flume.extension.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a facade that handle rpcclient close and exception
 * 
 * @author tongwei
 *
 */
public class RpcClientFacade {

	private static final Logger logger = LoggerFactory.getLogger(RpcClientFacade.class);

	private RpcClient client;
	private String hostname;
	private int port;
	private Properties clientProps;
	private int retryCount;

	public RpcClientFacade(String hostname, int port,int retryCount, Properties clientProps) {
		super();
		this.hostname = hostname;
		this.port = port;
		this.clientProps = clientProps;
		this.retryCount = retryCount;

		clientProps.setProperty("hosts", "h1");
		clientProps.setProperty("hosts.h1", hostname + ":" + port);
	}

	public void init() {
		clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
				RpcClientFactory.ClientType.THRIFT.name());
		client = RpcClientFactory.getInstance(clientProps);
	}

	public void sendDataToFlume(List<Event> events) throws EventDeliveryException{
		Throwable lastTrace = null;
		for(int i=0;i<retryCount;i++){
			try {
				client.appendBatch(events);
				return;
			} catch (Exception e) {
				lastTrace=e;
				
				client.close();
				client = null;
				clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
						RpcClientFactory.ClientType.THRIFT.name());
				
				client = RpcClientFactory.getInstance(clientProps);
				
				logger.info("reconnect to {}-{} ok", hostname, port);
			}
		}
		throw new EventDeliveryException(lastTrace);
	}

	public void cleanUp() {
		if (client != null) {
			client.close();
		}
	}
	
	public static void main(String[] args) throws EventDeliveryException {
		
		Properties clientProps = new Properties();
		
		clientProps.put("batch-size", 10000);
		clientProps.put("maxConnections", 1);
		
		RpcClientFacade clientFacade = new RpcClientFacade("10.13.56.52", 21212,3, clientProps);
		
		clientFacade.init();
		
		List<Event> events = new ArrayList<Event>();
		
	    Event event = new SimpleEvent();
	    event.setBody("hello".getBytes());
	    events.add(event);
		
		clientFacade.sendDataToFlume(events);
		
	}

}
