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
import io.netty.channel.ChannelPromise;
import io.netty.incubator.codec.http3.Http3GoAwayFrame;
import io.netty.util.ReferenceCountUtil;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.nested.Http3Config;

public class Http3TripleServerConnectionHandler extends ChannelDuplexHandler {
    private Http3GracefulShutdown gracefulShutdown;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Http3GoAwayFrame) {
            ReferenceCountUtil.release(msg);
            return;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (gracefulShutdown != null) {
            // Ensure clean shutdown if not already done
            gracefulShutdown.secondGoAwayAndClose(ctx);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (gracefulShutdown != null) {
            // Ensure clean shutdown on exceptions
            gracefulShutdown.secondGoAwayAndClose(ctx);
        }
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        if (gracefulShutdown == null) {
            // Initialize graceful shutdown with default message
            Http3Config config = ConfigManager.getProtocolOrDefault(null).getTripleOrDefault().getHttp3OrDefault();
            gracefulShutdown = new Http3GracefulShutdown(ctx, "Server shutting down", promise, config);
            gracefulShutdown.gracefulShutdown();
        } else {
            // If already shutting down, just pass through
            super.close(ctx, promise);
        }
    }
}
