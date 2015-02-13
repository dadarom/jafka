/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sohu.jafka.producer.async;

import static com.sohu.jafka.utils.Utils.getInt;
import static com.sohu.jafka.utils.Utils.getProps;
import static com.sohu.jafka.utils.Utils.getString;


import java.util.Properties;

import com.sohu.jafka.producer.SyncProducerConfig;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class AsyncProducerConfig extends SyncProducerConfig implements AsyncProducerConfigShared {


    public AsyncProducerConfig(Properties properties) {
        super(properties);
    }


    // 在该段时间内没有新消息到来的话，发送消息到broker
    public int getQueueTime() {
        return getInt(props, "queue.time", 5000);
    }

    public int getQueueSize() {
        return getInt(props, "queue.size", 10000);
    }

    /**
     * 1. 0表示队列未满时成功入队，否则丢弃。
     * 2. 小于0表示当队列满时，阻塞直到成功入队，
     * 3. 大于0表示等待这些ms都无法成功入队，舍弃
     * */
    public int getEnqueueTimeoutMs() {
        return getInt(props, "queue.enqueueTimeout.ms", 0);
    }

    // 指定异步发送每次批量发送的消息个数
    public int getBatchSize() {
        return getInt(props, "batch.size", 200);
    }


    public String getCbkHandler() {
        return getString(props, "callback.handler", null);
    }

    public Properties getCbkHandlerProperties() {
        return getProps(props, "callback.handler.props", null);
    }

    public String getEventHandler() {
        return getString(props, "event.handler", null);
    }

    public Properties getEventHandlerProperties() {
        return getProps(props, "event.handler.props", null);
    }

}
