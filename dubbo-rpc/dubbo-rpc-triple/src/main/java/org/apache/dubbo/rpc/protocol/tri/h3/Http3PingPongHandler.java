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
package org.apache.dubbo.rpc.protocol.tri.h3;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.incubator.codec.quic.QuicStreamChannel;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles ping-pong mechanism for HTTP/3 connections using QUIC's built-in ping frames.
 * This implementation adapts the HTTP/2 ping-pong pattern to work with QUIC's native
 * connection management features.
 */
public class Http3PingPongHandler extends ChannelDuplexHandler {
    private final long pingAckTimeout;
    private ScheduledFuture<?> pingAckTimeoutFuture;
    private Http3GracefulShutdown gracefulShutdown;

    public Http3PingPongHandler(long pingAckTimeout) {
        this.pingAckTimeout = pingAckTimeout;
    }

    /**
     * Sets the graceful shutdown handler to notify when ping timeouts occur
     */
    public void setGracefulShutdown(Http3GracefulShutdown gracefulShutdown) {
        this.gracefulShutdown = gracefulShutdown;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof IdleStateEvent)) {
            ctx.fireUserEventTriggered(evt);
            return;
        }

        // For HTTP/3, we use QUIC's built-in ping mechanism
        if (ctx.channel() instanceof QuicStreamChannel) {
            QuicStreamChannel quicChannel = (QuicStreamChannel) ctx.channel();
            // Schedule ping timeout handler
            if (pingAckTimeoutFuture == null) {
                pingAckTimeoutFuture = ctx.executor()
                    .schedule(new CloseChannelTask(ctx), pingAckTimeout, TimeUnit.MILLISECONDS);
            }
            // QUIC handles ping frame sending internally
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Cancel ping timeout on any read, as QUIC handles ping responses internally
        if (pingAckTimeoutFuture != null) {
            pingAckTimeoutFuture.cancel(true);
            pingAckTimeoutFuture = null;
            
            // Notify graceful shutdown if it's waiting for ping response
            if (gracefulShutdown != null) {
                gracefulShutdown.pingAckReceived();
            }
        }
        super.channelRead(ctx, msg);
    }

    private static class CloseChannelTask implements Runnable {
        private final ChannelHandlerContext ctx;

        CloseChannelTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (ctx.channel().isActive()) {
                ctx.close();
            }
        }
    }
}
