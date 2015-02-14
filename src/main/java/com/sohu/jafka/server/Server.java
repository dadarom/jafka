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

package com.sohu.jafka.server;

import com.sohu.jafka.http.HttpRequestHandler;
import com.sohu.jafka.http.HttpServer;
import com.sohu.jafka.log.LogManager;
import com.sohu.jafka.mx.ServerInfo;
import com.sohu.jafka.mx.SocketServerStats;
import com.sohu.jafka.network.SocketServer;
import com.sohu.jafka.utils.Mx4jLoader;
import com.sohu.jafka.utils.Scheduler;
import com.sohu.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The main server container
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Server implements Closeable {

    final String CLEAN_SHUTDOWN_FILE = ".jafka_cleanshutdown";

    final private Logger logger = LoggerFactory.getLogger(Server.class);

    final ServerConfig config;

    final Scheduler scheduler = new Scheduler(1, "jafka-logcleaner-", false);

    private LogManager logManager;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private SocketServer socketServer;
    private HttpServer httpServer;

    private final File logDir;

    private final ServerInfo serverInfo = new ServerInfo();

    //
    public Server(ServerConfig config) {
        this.config = config;
        logDir = new File(config.getLogDir());
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
    }

    public void startup() {
        try {
            final long start = System.currentTimeMillis();
            logger.info("Starting Jafka server(brokerid={}) {}" ,this.config.getBrokerId(),serverInfo.getVersion());
            Utils.registerMBean(serverInfo);
            boolean needRecovery = true;
            File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
            if (cleanShutDownFile.exists()) {
                needRecovery = false;
                cleanShutDownFile.delete();
            }
            //1. 初始化消息数据管理类LogManager，并将所有的消息数据按照一定格式读入内存（非数据内容本身）
            this.logManager = new LogManager(config,//
                    scheduler,//
                    1000L * 60 * config.getLogCleanupIntervalMinutes(),//
                    1000L * 60 * 60 * config.getLogRetentionHours(),//
                    needRecovery);
            this.logManager.setRollingStategy(config.getRollingStrategy());
            logManager.load();

            //2. 
            RequestHandlers handlers = new RequestHandlers(logManager);
            socketServer = new SocketServer(handlers, config);
            Utils.registerMBean(socketServer.getStats());
            socketServer.startup();
            
            //3. 
            final int httpPort = config.getHttpPort();
            if(httpPort>0){
                HttpRequestHandler httpRequestHandler = new HttpRequestHandler(logManager);
                httpServer = new HttpServer(httpPort,httpRequestHandler);
                httpServer.start();
            }

            Mx4jLoader.maybeLoad();
            
           //4. 如果开启了zookeeper连接，则将该broker信息注册到zookeeper中，并开启定时flush消息数据的线程
            /**
             * Registers this broker in ZK. After this, consumers can connect to broker. So
             * this should happen after socket server start.
             */
            logManager.startup();
            final long cost = (System.currentTimeMillis() - start) / 1000;
            logger.info("Jafka(brokerid={}) started at *:{}, cost {} seconds",config.getBrokerId(), config.getPort(), cost);
           
            //5. 
            serverInfo.started();
            
        } catch (Exception ex) {
            logger.error("========================================");
            logger.error("Fatal error during startup.", ex);
            logger.error("========================================");
            close();
        }
    }

    public void close() {
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (!canShutdown) return;//CLOSED

        logger.info("Shutting down Jafka server(brokerid={})...",this.config.getBrokerId());
        try {
            scheduler.shutdown();
            if (socketServer != null) {
                socketServer.close();
                Utils.unregisterMBean(socketServer.getStats());
            }
            if(httpServer != null){
                httpServer.close();
            }
            if (logManager != null) {
                logManager.close();
            }

            File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
            cleanShutDownFile.createNewFile();
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
        shutdownLatch.countDown();
        logger.info("Shutdown Jafka server(brokerid={}) completed",config.getBrokerId());

    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }


    public LogManager getLogManager() {
        return logManager;
    }

    public SocketServerStats getStats() {
        return socketServer.getStats();
    }

}
