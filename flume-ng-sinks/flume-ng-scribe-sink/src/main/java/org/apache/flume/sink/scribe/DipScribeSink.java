package org.apache.flume.sink.scribe;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Synchronous Sink that forwards messages to a scribe listener. <p>
 * add reconnect to the scribe sink.<p>
 * @author tongwei
 */
public class DipScribeSink extends AbstractSink implements Configurable {
	private static final Logger logger = LoggerFactory.getLogger(DipScribeSink.class);
	private long batchSize = 1;
	private SinkCounter sinkCounter;
	private FlumeEventSerializer serializer;
	private Boolean clientAlive;
	private Scribe.Client client;
	private String host;
	private int port;
	private TTransport trans;

	@Override
	public void configure(Context context) {
		String name = context.getString(ScribeSinkConfigurationConstants.CONFIG_SINK_NAME, "sink-" + hashCode());
		setName(name);
		sinkCounter = new SinkCounter(name);
		batchSize = context.getLong(ScribeSinkConfigurationConstants.CONFIG_BATCHSIZE, 1L);
		String clazz = context.getString(ScribeSinkConfigurationConstants.CONFIG_SERIALIZER,
				EventToLogEntrySerializer.class.getName());

		try {
			serializer = (FlumeEventSerializer) Class.forName(clazz).newInstance();
		} catch (Exception ex) {
			logger.warn("Defaulting to EventToLogEntrySerializer", ex);
			serializer = new EventToLogEntrySerializer();
		} finally {
			serializer.configure(context);
		}

		host = context.getString(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_HOST);
		port = context.getInteger(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_PORT);

	}

	@Override
	public synchronized void start() {
		super.start();

		try {
			createConnection();
			clientAlive = true;
		} catch (Exception e) {
			logger.error("Error connect to scribe server", e);
			clientAlive = false;
		}

		sinkCounter.start();
	}

	@Override
	public synchronized void stop() {
		super.stop();
		sinkCounter.stop();
		if (trans != null) {
			trans.close();
		}
		if (client != null) {
			client = null;
		}
	}

	@Override
	public Status process() throws EventDeliveryException {
		logger.debug("start processing");

		Status status = Status.READY;
		Channel channel = getChannel();
		List<LogEntry> eventList = new ArrayList<LogEntry>();
		Transaction transaction = null;
		try {
			transaction = channel.getTransaction();
			transaction.begin();

			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					sinkCounter.incrementBatchUnderflowCount();
					break;
				} else {
					eventList.add(serializer.serialize(event));
				}
			}

			if (eventList.size() > 0) {

				if (!clientAlive) {
					try {
						createConnection();
						clientAlive = true;
					} catch (Exception e) {
						throw new EventDeliveryException("Failed to send event due to lazy create connection error. ",
								e);
					}
				}
				
				try{
					
					sendEvents(eventList);
					
				}catch(TException e){
					
					closeConnection();
					clientAlive = false;
					throw new EventDeliveryException("connect error:",e);
					
				}
			}else{
				status = Status.BACKOFF;
			}
			
			transaction.commit();

		} catch (Exception ex) {

			String errorMsg = "Failed to publish events";
			logger.error("Failed to publish events", ex);
			status = Status.BACKOFF;
			if (transaction != null) {
				try {
					transaction.rollback();
				} catch (Exception e) {
					logger.error("Transaction rollback failed", e);
					throw Throwables.propagate(e);
				}
			}
			throw new EventDeliveryException(errorMsg, ex);

		} finally {

			if (transaction != null) {

				transaction.close();
			}
		}

		return status;
	}

	private void createConnection() throws Exception {

		trans = new TFramedTransport(new TSocket(new Socket(host, port)));
		client = new Scribe.Client(new TBinaryProtocol(trans, false, false));

	}

	private void closeConnection() {
		trans.close();
		client = null;
	}

	private void sendEvents(List<LogEntry> eventList) throws TException, EventDeliveryException{
		sinkCounter.addToEventDrainAttemptCount(eventList.size());
		ResultCode rc = client.Log(eventList);
		if (!rc.equals(ResultCode.OK)) {
			throw new EventDeliveryException("Failed to send event due to lazy create connection error. ");
		}

	}
}