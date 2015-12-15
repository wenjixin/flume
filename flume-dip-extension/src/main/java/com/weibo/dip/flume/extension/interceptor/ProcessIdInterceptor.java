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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Interceptor class that sets the host name or IP on all events that are
 * intercepted.
 * <p>
 * The host header is named <code>host</code> and its format is either the FQDN
 * or IP of the host on which this interceptor is run.
 *
 *
 * Properties:
 * <p>
 *
 * preserveExisting: Whether to preserve an existing value for 'host' (default
 * is false)
 * <p>
 *
 * useIP: Whether to use IP address or fully-qualified hostname for 'host'
 * header value (default is true)
 * <p>
 *
 * hostHeader: Specify the key to be used in the event header map for the host
 * name. (default is "host")
 * <p>
 *
 * Sample config:
 * <p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = com.weibo.dip.flume.extension.interceptor.ProcessIdInterceptor$Builder<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = true<p>
 *   agent.sources.r1.interceptors.i1.pidHeader = pid<p>
 * </code>
 *
 */
public class ProcessIdInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessIdInterceptor.class);

    private final boolean preserveExisting;
    private final String header;
    private String pid = null;

    /**
     * Only {@link ProcessIdInterceptor.Builder} can build me
     */
    private ProcessIdInterceptor(boolean preserveExisting, String header) {
        this.preserveExisting = preserveExisting;
        this.header = header;
        this.pid = getPid();
        logger.info("Pid is {}", pid);
    }

    @Override
    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        if (preserveExisting && headers.containsKey(header)) {
            return event;
        }
        if (pid != null) {
            headers.put(header, pid);
        }

        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * 
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instances of the HostInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private boolean preserveExisting = Constants.PRESERVE_DFLT;
        private String header = Constants.PID;

        @Override
        public Interceptor build() {
            return new ProcessIdInterceptor(preserveExisting, header);
        }

        @Override
        public void configure(Context context) {
            preserveExisting = context.getBoolean(Constants.PRESERVE, Constants.PRESERVE_DFLT);
            header = context.getString(Constants.PID_HEADER, Constants.PID);
        }

    }

    public static class Constants {
        public static String PID = "pid";

        public static String PRESERVE = "preserveExisting";
        public static boolean PRESERVE_DFLT = false;

        public static String PID_HEADER = "pid";
    }

    public static String getPid() {
        // get name representing the running Java virtual machine.
        String name = ManagementFactory.getRuntimeMXBean().getName();

        // get pid
        String pid = name.split("@")[0];

        return pid;
    }

}
