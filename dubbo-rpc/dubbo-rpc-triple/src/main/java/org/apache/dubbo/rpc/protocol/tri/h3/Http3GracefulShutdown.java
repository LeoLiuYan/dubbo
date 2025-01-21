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

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3Headers;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3GoAwayFrame;
import io.netty.incubator.codec.http3.Http3Headers;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInitializer;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_RESPONSE;

public class Http3GracefulShutdown {
    private static final ErrorTypeAwareLogger LOGGER = LoggerFactory.getErrorTypeAwareLogger(Http3GracefulShutdown.class);
    private static final long GRACEFUL_SHUTDOWN_PING = 0x97ACEF001L;
    private static final long GRACEFUL_SHUTDOWN_PING_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final ChannelHandlerContext ctx;
    private final String goAwayMessage;
    private final ChannelPromise originPromise;
    private ScheduledFuture<?> pingFuture;

    public Http3GracefulShutdown(ChannelHandlerContext ctx, String goAwayMessage, ChannelPromise originPromise) {
        this.ctx = ctx;
        this.goAwayMessage = goAwayMessage;
        this.originPromise = originPromise;
    }

    public void gracefulShutdown() {
        // Send initial GOAWAY to prevent new streams
        Http3GoAwayFrame goAwayFrame = new Http3GoAwayFrame(0);
        ctx.writeAndFlush(goAwayFrame);

        // Schedule ping timeout
        pingFuture = ctx.executor().schedule(
            () -> secondGoAwayAndClose(ctx),
            GRACEFUL_SHUTDOWN_PING_TIMEOUT_NANOS,
            TimeUnit.NANOSECONDS);

        // Send ping on control stream
        sendPing();
    }

    private void sendPing() {
        Http3.newRequestStream(ctx.channel(), new Http3RequestStreamInitializer() {
            @Override
            protected void initRequestStream(QuicStreamChannel ch) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.path("*");
                headers.method("OPTIONS");
                headers.scheme("https");
                headers.set("tri-ping", String.valueOf(GRACEFUL_SHUTDOWN_PING));

                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers))
                    .addListener(future -> {
                        if (!future.isSuccess()) {
                            LOGGER.warn(PROTOCOL_FAILED_RESPONSE, "", "", 
                                "Failed to send ping frame for graceful shutdown", future.cause());
                        }
                        ch.close();
                    });
            }
        }).addListener(future -> {
            if (!future.isSuccess()) {
                LOGGER.warn(PROTOCOL_FAILED_RESPONSE, "", "", 
                    "Failed to create control stream for graceful shutdown", future.cause());
                secondGoAwayAndClose(ctx);
            }
        });
    }

    public void secondGoAwayAndClose(ChannelHandlerContext ctx) {
        if (pingFuture != null) {
            pingFuture.cancel(false);
            pingFuture = null;
        }

        // Send final GOAWAY and close
        Http3GoAwayFrame goAwayFrame = new Http3GoAwayFrame(0);
        ctx.writeAndFlush(goAwayFrame);
        ctx.close(originPromise);
    }

    public void pingAckReceived() {
        if (pingFuture != null) {
            pingFuture.cancel(false);
            pingFuture = null;
            secondGoAwayAndClose(ctx);
        }
    }
}
