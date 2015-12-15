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
 *   agent.sources.r1.interceptors.i1.type = com.weibo.dip.flume.extension.interceptor.HeaderCopyInterceptor$Builder<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = true<p>
 *   agent.sources.r1.interceptors.i1.from = fromHeader<p>
 *   agent.sources.r1.interceptors.i1.to = toHeader<p>
 * </code>
 * 
 * @author tongwei
 * @date 2015年12月1日 下午5:05:40
 * @desc
 */
public class HeaderCopyInterceptor implements Interceptor {

    private final String from;
    private final String to;
    private final boolean preserveExisting;

    public HeaderCopyInterceptor(String from, String to, boolean preserveExisting) {
        super();
        this.from = from;
        this.to = to;
        this.preserveExisting = preserveExisting;
    }

    @Override
    public void initialize() {
        // pass
    }

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();

        if (preserveExisting && headers.containsKey(to)) {
            return event;
        }

        String fromValue = headers.get(from);

        if (fromValue != null) {
            headers.put(to, fromValue);
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

        private static final String FROM = "from";
        private static final String TO = "to";
        private static final String PRESERVE = "preserveExisting";
        private static final boolean PRESERVE_DFLT = false;

        private String from;
        private String to;
        private boolean preserveExisting = PRESERVE_DFLT;

        @Override
        public void configure(Context context) {
            from = context.getString(FROM);
            to = context.getString(TO);
            preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
        }

        @Override
        public Interceptor build() {
            Preconditions.checkNotNull(from, "From header required");
            Preconditions.checkNotNull(to, "TO header required");
            return new HeaderCopyInterceptor(from, to, preserveExisting);
        }

    }
}
