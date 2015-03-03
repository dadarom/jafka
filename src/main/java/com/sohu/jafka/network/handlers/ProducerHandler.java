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

package com.sohu.jafka.network.handlers;

import static java.lang.String.format;

import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.api.RequestKeys;
import com.sohu.jafka.log.ILog;
import com.sohu.jafka.log.LogManager;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.mx.BrokerTopicStat;
import com.sohu.jafka.network.Receive;
import com.sohu.jafka.network.Send;

/**
 * handler for producer request
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ProducerHandler extends AbstractHandler {

    final String errorFormat = "Error processing %s on %s:%d";

    public ProducerHandler(LogManager logManager) {
        super(logManager);
    }

    public Send handler(RequestKeys requestType, Receive receive) {
        final long st = System.currentTimeMillis();
        
        ProducerRequest request = ProducerRequest.readFrom(receive.buffer());
        if (logger.isDebugEnabled()) {
            logger.debug("Producer request " + request.toString());
        }
        handleProducerRequest(request);
        long et = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("produce a message(set) cost " + (et - st) + " ms");
        }
        return null;
    }

    protected void handleProducerRequest(ProducerRequest request) {
        int partition = request.getTranslatedPartition(logManager);
        try {
            final ILog log = logManager.getOrCreateLog(request.topic, partition);
          
            
          //=================================================
            /**
             * 将client socket流内producer request的数据写入LogSegment的file channel.
             * 注意这里的messages是ByteBufferMessageSet
             */
            
            /***
             * 技巧点： 注意这里 ProducerRequest的 ByteBufferMessageSet内的 byte buffer
             * 		   是和 从client socket读入后形成的byte buffer是同一内存buffer
             * 
             * 即： 从socket channel 最终到 file channel时，只有一次byte buffer的复制！！！
             * 
             * 但为啥不利用fileChannel.transferFrom(socketChannel)直接写文件？？？数据量较多？数据需要分析（根据size读数据量）且不是全部写，只写content部分？
             */
            log.append(request.messages);
          //=================================================
            
            long messageSize = request.messages.getSizeInBytes();
            if (logger.isDebugEnabled()) {
                logger.debug(messageSize + " bytes written to logs " + log);
                for (MessageAndOffset m : request.messages) {
                    logger.trace("wrote message " + m.offset + " to disk");
                }
            }
            BrokerTopicStat.getInstance(request.topic).recordBytesIn(messageSize);
            BrokerTopicStat.getBrokerAllTopicStat().recordBytesIn(messageSize);
        } catch (RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            BrokerTopicStat.getInstance(request.topic).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw e;
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            BrokerTopicStat.getInstance(request.topic).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
