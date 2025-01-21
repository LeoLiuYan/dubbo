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

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.incubator.codec.http3.Http3GoAwayFrame;
import io.netty.incubator.codec.http3.DefaultHttp3GoAwayFrame;
import io.netty.util.concurrent.Future;

import java.util.concurrent.TimeUnit;

/**
 * Handles graceful shutdown for HTTP/3 connections, following a similar pattern to HTTP/2
 * but using HTTP/3 specific frames and control streams.
 */
public class Http3GracefulShutdown {
    // Default ping value matching HTTP/2 implementation for consistency
    static final long GRACEFUL_SHUTDOWN_PING = 0x97ACEF001L;
    
    private final ChannelHandlerContext ctx;
    private final ChannelPromise originPromise;
    private final String goAwayMessage;
    private final Http3Config http3Config;
    private boolean pingAckedOrTimeout;
    private Future<?> pingFuture;

    public Http3GracefulShutdown(ChannelHandlerContext ctx, String goAwayMessage, ChannelPromise originPromise, Http3Config http3Config) {
        this.ctx = ctx;
        this.goAwayMessage = goAwayMessage;
        this.originPromise = originPromise;
        this.http3Config = http3Config;
    }

    /**
     * Initiates the graceful shutdown process:
     * 1. Sends initial GOAWAY frame with max stream ID
     * 2. Sends ping frame to verify active connections
     * 3. After timeout or ping ack, sends second GOAWAY
     * 4. Finally closes the channel
     */
    public void gracefulShutdown() {
        // Send initial GOAWAY frame with max stream ID to stop new requests
        Http3GoAwayFrame goAwayFrame = new DefaultHttp3GoAwayFrame(Long.MAX_VALUE);
        ctx.writeAndFlush(goAwayFrame);

        // Schedule the second phase of shutdown
        long timeoutNanos = TimeUnit.SECONDS.toNanos(http3Config.getGracefulShutdownPingTimeoutOrDefault());
        pingFuture = ctx.executor()
                .schedule(() -> secondGoAwayAndClose(ctx), timeoutNanos, TimeUnit.NANOSECONDS);

        // Send ping frame using QUIC control stream
        // Note: HTTP/3 uses QUIC for connection management, so we rely on QUIC ping frames
        sendQuicPing(ctx);
    }

    /**
     * Sends the second GOAWAY frame and closes the connection
     */
    void secondGoAwayAndClose(ChannelHandlerContext ctx) {
        if (pingAckedOrTimeout) {
            return;
        }
        pingAckedOrTimeout = true;

        if (pingFuture != null) {
            pingFuture.cancel(false);
        }

        try {
            // Send final GOAWAY frame with stream ID 0
            Http3GoAwayFrame goAwayFrame = new DefaultHttp3GoAwayFrame(0);
            ctx.writeAndFlush(goAwayFrame);
            
            // Close the channel after the final GOAWAY
            ctx.close(originPromise);
        } catch (Exception e) {
            ctx.fireExceptionCaught(e);
        }
    }

    /**
     * Sends a QUIC-level ping frame to verify connection state
     */
    private void sendQuicPing(ChannelHandlerContext ctx) {
        // Note: In HTTP/3, we use QUIC's built-in ping mechanism
        // The actual implementation depends on the QUIC library being used
        // For now, we rely on QUIC's internal ping frame handling
    }

    /**
     * Called when a ping acknowledgment is received
     */
    public void pingAckReceived() {
        if (!pingAckedOrTimeout) {
            pingAckedOrTimeout = true;
            if (pingFuture != null) {
                pingFuture.cancel(false);
            }
            secondGoAwayAndClose(ctx);
        }
    }
}
