/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.scribe;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Not really a unit test
 */
public class ScribeSinkTest {
	private AsyncScribeSink sink = new AsyncScribeSink();

	@Before
	public void setUp() throws Exception {
		Context ctx = new Context();
		ctx.put(ScribeSinkConfigurationConstants.CONFIG_SERIALIZER, EventToLogEntrySerializer.class.getName());
		ctx.put(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_HOST, "127.0.0.1");
		ctx.put(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_PORT, "1463");
		ctx.put(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY_HEADER,
				ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY);
		sink.configure(ctx);
		PseudoTxnMemoryChannel c = new PseudoTxnMemoryChannel();
		c.configure(ctx);
		c.start();
		sink.setChannel(c);
		sink.start();
	}

	@After
	public void tearDown() throws Exception {
		Thread.sleep(1000);
		sink.getChannel().stop();
		sink.stop();
	}

	@Test
	public void testProcess() throws Exception {
		Event e = new SimpleEvent();
		e.getHeaders().put(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY, "default1");
		e.setBody("This is test ".getBytes());
		sink.getChannel().put(e);
		sink.process();
	}

	public static void main(String[] args)
			throws UnknownHostException, IOException, InterruptedException, TException {
		String host = "10.13.56.52";
		int port = 1466;

		Scribe.Client client;
		TTransport trans;

		trans = new TFramedTransport(new TSocket(new Socket(host, port)));
		client = new Scribe.Client(new TBinaryProtocol(trans, false, false));

		while (true) {

			List<LogEntry> logEntries = new ArrayList<LogEntry>();

			for (int i = 0; i < 1000; i++) {
				logEntries.add(new LogEntry("app_weibomobilekafka1234_sinatrans",ByteBuffer.wrap("Hello".getBytes())));
			}
			client.Log(logEntries);

			TimeUnit.SECONDS.sleep(1);
		}

	}
}
