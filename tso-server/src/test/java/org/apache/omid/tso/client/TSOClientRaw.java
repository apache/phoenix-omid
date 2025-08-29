/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso.client;

import org.apache.omid.protobuf.OmidProtobufDecoder;
import org.apache.omid.protobuf.OmidProtobufEncoder;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.SettableFuture;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.proto.TSOProto.Response;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * Raw client for communicating with tso server directly with protobuf messages
 */
public class TSOClientRaw {

    private static final Logger LOG = LoggerFactory.getLogger(TSOClientRaw.class);

    private final BlockingQueue<SettableFuture<Response>> responseQueue
            = new ArrayBlockingQueue<SettableFuture<Response>>(5);
    private final Channel channel;

    public TSOClientRaw(String host, int port) throws InterruptedException, ExecutionException {

        InetSocketAddress addr = new InetSocketAddress(host, port);

        // Start client with Nb of active threads = 3
        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoclient-worker-%d").build();
        EventLoopGroup workerGroup = new NioEventLoopGroup(3, workerThreadFactory);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(8 * 1024, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("protobufdecoder", new OmidProtobufDecoder(TSOProto.Response.getDefaultInstance()));
                pipeline.addLast("protobufencoder", new OmidProtobufEncoder());
                pipeline.addLast("rawHandler", new RawHandler());
            }
        });
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100);

        ChannelFuture channelFuture = bootstrap.connect(addr).await();
        channel = channelFuture.channel();

    }

    public void write(TSOProto.Request request) {
        channel.writeAndFlush(request);
    }

    public Future<Response> getResponse() throws InterruptedException {
        SettableFuture<Response> future = SettableFuture.create();
        responseQueue.put(future);
        return future;
    }

    public void close() throws InterruptedException {
        responseQueue.put(SettableFuture.<Response>create());
        channel.close();
    }

    private class RawHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            LOG.info("Message received", msg);
            if (msg instanceof Response) {
                Response resp = (Response) msg;
                try {
                    SettableFuture<Response> future = responseQueue.take();
                    future.set(resp);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted in handler", ie);
                }
            } else {
                LOG.warn("Received unknown message", msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.info("Exception received", cause);
            try {
                SettableFuture<Response> future = responseQueue.take();
                future.setException(cause);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted handling exception", ie);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx)
                throws Exception {
            LOG.info("Inactive");
            try {
                SettableFuture<Response> future = responseQueue.take();
                future.setException(new ConnectionException());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted handling exception", ie);
            }
        }
    }
}
