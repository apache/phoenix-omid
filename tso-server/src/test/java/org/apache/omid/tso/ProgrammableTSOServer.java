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
package org.apache.omid.tso;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.tso.ProgrammableTSOServer.Response.ResponseType;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ThreadFactory;

/**
 * Used in tests. Allows to program the set of responses returned by a TSO
 */
@Sharable
public class ProgrammableTSOServer extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProgrammableTSOServer.class);

    private ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private Queue<Response> responseQueue = new LinkedList<>();

    private static final AttributeKey<TSOChannelContext> TSO_CTX =
            AttributeKey.valueOf("TSO_CTX");

    @Inject
    public ProgrammableTSOServer(int port) {
        // Setup netty listener

        int workerThreadCount = (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2;
        ThreadFactory bossThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoserver-boss-%d").build();
        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoserver-worker-%d").build();
        EventLoopGroup workerGroup = new NioEventLoopGroup(workerThreadCount, workerThreadFactory);
        EventLoopGroup bossGroup = new NioEventLoopGroup(bossThreadFactory);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup,  workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("protobufdecoder", new ProtobufDecoder(TSOProto.Request.getDefaultInstance()));
                pipeline.addLast("protobufencoder", new ProtobufEncoder());
                pipeline.addLast("handler", ProgrammableTSOServer.this);
            }
        });

        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(port)).syncUninterruptibly().channel();
        allChannels.add(channel);

        LOG.info("********** Dumb TSO Server running on port {} **********", port);
    }

    // ************************* Main interface for tests *********************

    /**
     * Allows to add response to the queue of responses
     *
     * @param r
     *            the response to add
     */
    public void queueResponse(Response r) {
        responseQueue.add(r);
    }

    /**
     * Removes all the current responses in the queue
     */
    public void cleanResponses() {
        responseQueue.clear();
    }

    // ******************** End of Main interface for tests *******************

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        allChannels.add(ctx.channel());
    }

    /**
     * Handle received messages
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof TSOProto.Request) {
            TSOProto.Request request = (TSOProto.Request) msg;
            Channel channel = ctx.channel();
            if (request.hasHandshakeRequest()) {
                checkHandshake(ctx, request.getHandshakeRequest());
                return;
            }
            if (!handshakeCompleted(ctx)) {
                LOG.info("handshake not completed");
                channel.close();
            }

            Response resp = responseQueue.poll();
            if (request.hasTimestampRequest()) {
                if (resp == null || resp.type != ResponseType.TIMESTAMP) {
                    throw new IllegalStateException("Expecting TS response to send but got " + resp);
                }
                TimestampResponse tsResp = (TimestampResponse) resp;
                sendTimestampResponse(tsResp.startTS, channel);
            } else if (request.hasCommitRequest()) {
                if (resp == null) {
                    throw new IllegalStateException("Expecting COMMIT response to send but got null");
                }
                switch (resp.type) {
                    case COMMIT:
                        CommitResponse commitResp = (CommitResponse) resp;
                        sendCommitResponse(commitResp.startTS, commitResp.commitTS, channel);
                        break;
                    case ABORT:
                        AbortResponse abortResp = (AbortResponse) resp;
                        sendAbortResponse(abortResp.startTS, channel);
                        break;
                    default:
                        throw new IllegalStateException("Expecting COMMIT response to send but got " + resp.type);
                }
            } else {
                LOG.error("Invalid request {}", request);
                ctx.channel().close();
            }
        } else {
            LOG.error("Unknown message type", msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof ClosedChannelException) {
            return;
        }
        LOG.warn("TSOHandler: Unexpected exception.", cause);
        ctx.channel().close();
    }

    private void checkHandshake(final ChannelHandlerContext ctx, TSOProto.HandshakeRequest request) {
        TSOProto.HandshakeResponse.Builder response = TSOProto.HandshakeResponse.newBuilder();
        if (request.hasClientCapabilities()) {

            response.setClientCompatible(true).setServerCapabilities(TSOProto.Capabilities.newBuilder().build());
            TSOChannelContext tsoCtx = new TSOChannelContext();
            tsoCtx.setHandshakeComplete();
            ctx.channel().attr(TSO_CTX).set(tsoCtx);
        } else {
            response.setClientCompatible(false);
        }
        ctx.channel().writeAndFlush(TSOProto.Response.newBuilder().setHandshakeResponse(response.build()).build());
    }

    private boolean handshakeCompleted(ChannelHandlerContext ctx) {
        Object o = ctx.channel().attr(TSO_CTX).get();
        if (o instanceof TSOChannelContext) {
            TSOChannelContext tsoCtx = (TSOChannelContext) o;
            return tsoCtx.getHandshakeComplete();
        }
        return false;
    }

    private void sendTimestampResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.writeAndFlush(builder.build());
    }

    private void sendCommitResponse(long startTimestamp, long commitTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(false).setStartTimestamp(startTimestamp).setCommitTimestamp(commitTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.writeAndFlush(builder.build());
    }

    private void sendAbortResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true).setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.writeAndFlush(builder.build());
    }

    private static class TSOChannelContext {
        boolean handshakeComplete;

        TSOChannelContext() {
            handshakeComplete = false;
        }

        boolean getHandshakeComplete() {
            return handshakeComplete;
        }

        void setHandshakeComplete() {
            handshakeComplete = true;
        }
    }

    public static class TimestampResponse extends Response {

        final long startTS;

        public TimestampResponse(long startTS) {
            super(ResponseType.TIMESTAMP);
            this.startTS = startTS;
        }

    }

    public static class CommitResponse extends Response {

        final long startTS;
        final long commitTS;

        public CommitResponse(long startTS, long commitTS) {
            super(ResponseType.COMMIT);
            this.startTS = startTS;
            this.commitTS = commitTS;
        }

    }

    public static class AbortResponse extends Response {

        final long startTS;

        public AbortResponse(long startTS) {
            super(ResponseType.ABORT);
            this.startTS = startTS;
        }

    }

    abstract static class Response {

        enum ResponseType {
            TIMESTAMP, COMMIT, ABORT
        }

        final ResponseType type;

        public Response(ResponseType type) {
            this.type = type;
        }

    }

}
