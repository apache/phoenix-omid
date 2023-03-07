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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;

import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.proto.TSOProto;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.netty.handler.ssl.OptionalSslHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.omid.tls.X509Util;
import org.apache.omid.tls.X509Exception;


/**
 * ChannelHandler for the TSO Server.
 *
 * Incoming requests are processed in this class
 */
// Marked sharable, as all global members used in callbacks are singletons.
@Sharable
public class TSOChannelHandler extends ChannelInboundHandlerAdapter implements Closeable {

    private final Logger LOG = LoggerFactory.getLogger(TSOChannelHandler.class);

    private final ServerBootstrap bootstrap;

    @VisibleForTesting
    Channel listeningChannel;
    @VisibleForTesting
    ChannelGroup allChannels;

    private RequestProcessor requestProcessor;

    private TSOServerConfig config;

    private MetricsRegistry metrics;

    private static final AttributeKey<TSOChannelContext> TSO_CTX =
            AttributeKey.valueOf("TSO_CTX");

    @Inject
    public TSOChannelHandler(TSOServerConfig config, RequestProcessor requestProcessor, MetricsRegistry metrics) {

        this.config = config;
        this.metrics = metrics;
        this.requestProcessor = requestProcessor;

        // Setup netty listener
        int workerThreadCount= (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2;
        ThreadFactory bossThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoserver-boss-%d").build();
        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoserver-worker-%d").build();
        EventLoopGroup workerGroup = new NioEventLoopGroup(workerThreadCount, workerThreadFactory);
        EventLoopGroup bossGroup = new NioEventLoopGroup(bossThreadFactory);

        this.bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup,  workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                // Max packet length is 10MB. Transactions with so many cells
                // that the packet is rejected will receive a ServiceUnavailableException.
                // 10MB is enough for 2 million cells in a transaction though.
                if (config.getTlsEnabled())
                {
                    initSSL(pipeline, config.getSupportPlainText());
                }
                pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("protobufdecoder", new ProtobufDecoder(TSOProto.Request.getDefaultInstance()));
                pipeline.addLast("protobufencoder", new ProtobufEncoder());
                pipeline.addLast("handler", TSOChannelHandler.this);
            }
        });
    }

    private void initSSL(ChannelPipeline p, boolean supportPlaintext)
            throws X509Exception, IOException {
        String keyStoreLocation = config.getKeyStoreLocation();
        char[] keyStorePassword = config.getKeyStorePassword().toCharArray();
        String keyStoreType = config.getKeyStoreType();

        String trustStoreLocation = config.getTrustStoreLocation();
        char[] truststorePassword = config.getTrustStorePassword().toCharArray();
        String truststoreType = config.getTrustStoreType();

        boolean sslCrlEnabled = config.getSslCrlEnabled();
        boolean sslOcspEnabled = config.getSslOcspEnabled();

        String enabledProtocols = config.getEnabledProtocols();
        String cipherSuites =  config.getCipherSuites();

        String tlsConfigProtocols = config.getTsConfigProtocols();

        SslContext nettySslContext = X509Util.createSslContextForServer(keyStoreLocation, keyStorePassword,
                keyStoreType, trustStoreLocation, truststorePassword, truststoreType, sslCrlEnabled,
                sslOcspEnabled, enabledProtocols, cipherSuites, tlsConfigProtocols);


        if (supportPlaintext) {
            p.addLast("ssl", new OptionalSslHandler(nettySslContext));
            LOG.info("Dual mode SSL handler added for channel: {}", p.channel());
        } else {
            p.addLast("ssl", nettySslContext.newHandler(p.channel().alloc()));
            LOG.info("SSL handler added for channel: {}", p.channel());
        }
    }


    /**
     * Allows to create and connect the communication channel closing the previous one if existed
     */
    void reconnect() {
        if (listeningChannel == null && allChannels == null) {
            LOG.debug("Creating communication channel...");
        } else {
            LOG.debug("Reconnecting communication channel...");
            closeConnection();
        }
        // Create the global ChannelGroup
        allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        LOG.debug("\tCreating channel to listening for incoming connections in port {}", config.getPort());
        listeningChannel = bootstrap.bind(new InetSocketAddress(config.getPort())).syncUninterruptibly().channel();
        allChannels.add(listeningChannel);
        LOG.debug("\tListening channel created and connected: {}", listeningChannel);
    }

    /**
     * Allows to close the communication channel
     */
    void closeConnection() {
        LOG.debug("Closing communication channel...");
        if (allChannels != null) {
            LOG.debug("\tClosing channel group {}", allChannels);
            allChannels.close().awaitUninterruptibly();
            LOG.debug("\tChannel group {} closed", allChannels);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Netty SimpleChannelHandler implementation
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        allChannels.add(ctx.channel());
        LOG.debug("TSO channel active: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //ChannelGroup will automatically remove closed Channels
        LOG.debug("TSO channel inactive: {}", ctx.channel());
    }

    /**
     * Handle received messages
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof TSOProto.Request) {
            TSOProto.Request request = (TSOProto.Request) msg;
            if (request.hasHandshakeRequest()) {
                checkHandshake(ctx, request.getHandshakeRequest());
                return;
            }
            if (!handshakeCompleted(ctx)) {
                LOG.error("Handshake not completed. Closing channel {}", ctx.channel());
                ctx.channel().close();
            }

            if (request.hasTimestampRequest()) {
                requestProcessor.timestampRequest(ctx.channel(), MonitoringContextFactory.getInstance(config,metrics));
            } else if (request.hasCommitRequest()) {
                TSOProto.CommitRequest cr = request.getCommitRequest();
                requestProcessor.commitRequest(cr.getStartTimestamp(),
                                               cr.getCellIdList(),
                                               cr.getTableIdList(),
                                               cr.getIsRetry(),
                                               ctx.channel(),
                                               MonitoringContextFactory.getInstance(config,metrics));
            } else if (request.hasFenceRequest()) {
                TSOProto.FenceRequest fr = request.getFenceRequest();
                requestProcessor.fenceRequest(fr.getTableId(),
                        ctx.channel(),
                        MonitoringContextFactory.getInstance(config,metrics));
            } else {
                LOG.error("Invalid request {}. Closing channel {}", request, ctx.channel());
                ctx.channel().close();
            }
        } else {
            LOG.error("Unknown message type", msg);
        }
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof ClosedChannelException) {
            LOG.warn("ClosedChannelException caught. Cause: ", cause);
            return;
        }
        LOG.warn("Unexpected exception. Closing channel {}", ctx.channel(), cause);
        ctx.channel().close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Closeable implementation
    // ----------------------------------------------------------------------------------------------------------------
    @Override
    public void close() throws IOException {
        LOG.debug("Shutting down communication channel...");
        bootstrap.config().group().shutdownGracefully();
        bootstrap.config().childGroup().shutdownGracefully();

        bootstrap.config().group().terminationFuture().awaitUninterruptibly();
        bootstrap.config().childGroup().terminationFuture().awaitUninterruptibly();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods and classes
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Contains the required context for handshake
     */
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

    private void checkHandshake(final ChannelHandlerContext ctx, TSOProto.HandshakeRequest request) {

        TSOProto.HandshakeResponse.Builder response = TSOProto.HandshakeResponse.newBuilder();
        if (request.hasClientCapabilities()) {

            response.setClientCompatible(true)
                    .setServerCapabilities(TSOProto.Capabilities.newBuilder().build());
            TSOChannelContext tsoCtx = new TSOChannelContext();
            tsoCtx.setHandshakeComplete();
            ctx.channel().attr(TSO_CTX).set(tsoCtx);
        } else {
            response.setClientCompatible(false);
        }
        response.setLowLatency(config.getLowLatency());
        ctx.channel().writeAndFlush(TSOProto.Response.newBuilder().setHandshakeResponse(response.build()).build());

    }

    private boolean handshakeCompleted(ChannelHandlerContext ctx) {

        TSOChannelContext tsoCtx = ctx.channel().attr(TSO_CTX).get();
        if (tsoCtx != null) {
            return tsoCtx.getHandshakeComplete();
        }
        return false;
    }

}
