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

import org.apache.omid.NetworkUtils;
import org.apache.omid.protobuf.OmidProtobufDecoder;
import org.apache.omid.protobuf.OmidProtobufEncoder;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.proto.TSOProto;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@SuppressWarnings({"UnusedDeclaration", "StatementWithEmptyBody"})
public class TestTSOChannelHandlerNetty {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOChannelHandlerNetty.class);

    @Mock
    private
    RequestProcessor requestProcessor;

    @BeforeMethod
    public void beforeTestMethod() {
        MockitoAnnotations.initMocks(this);
    }

    private TSOChannelHandler getTSOChannelHandler(int port) {
        TSOServerConfig config = new TSOServerConfig();
        config.setPort(port);
        return new TSOChannelHandler(config, requestProcessor, new NullMetricsProvider());
    }

    @Test(timeOut = 10_000)
    public void testMainAPI() throws Exception {
        int port = NetworkUtils.getFreePort();
        TSOChannelHandler channelHandler = getTSOChannelHandler(port);
        try {
            // Check initial state
            assertNull(channelHandler.listeningChannel);
            assertNull(channelHandler.allChannels);

            // Check initial connection
            channelHandler.reconnect();
            assertTrue(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 1);
            assertEquals(((InetSocketAddress) channelHandler.listeningChannel.localAddress()).getPort(), port);

            // Check connection close
            channelHandler.closeConnection();
            assertFalse(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 0);

            // Check re-closing connection
            channelHandler.closeConnection();
            assertFalse(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 0);

            // Check connection after closing
            channelHandler.reconnect();
            assertTrue(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 1);

            // Check re-connection
            channelHandler.reconnect();
            assertTrue(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 1);

            // Exercise closeable with re-connection trial
            channelHandler.close();
            assertFalse(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 0);
            try {
                channelHandler.reconnect();
                fail("Can't reconnect after closing");
            } catch (Exception e) {
                // Expected: Can't reconnect after closing
                assertFalse(channelHandler.listeningChannel.isOpen());
                assertEquals(channelHandler.allChannels.size(), 0);
            }
        } finally {
            if(channelHandler != null) channelHandler.close();
        }
    }

    @Test(timeOut = 10_000)
    public void testNettyConnectionToTSOFromClient() throws Exception {
        int port = NetworkUtils.getFreePort();
        TSOChannelHandler channelHandler = getTSOChannelHandler(port);
        try {
            Bootstrap nettyClient = createNettyClientBootstrap();

            ChannelFuture channelF = nettyClient.connect(new InetSocketAddress("localhost", port));

            // ------------------------------------------------------------------------------------------------------------
            // Test the client can't connect cause the server is not there
            // ------------------------------------------------------------------------------------------------------------
            while (!channelF.isDone()) /** do nothing */ ;
            assertFalse(channelF.isSuccess());

            // ------------------------------------------------------------------------------------------------------------
            // Test creation of a server connection
            // ------------------------------------------------------------------------------------------------------------
            channelHandler.reconnect();
            assertTrue(channelHandler.listeningChannel.isOpen());
            // Eventually the channel group of the server should contain the listening channel
            assertEquals(channelHandler.allChannels.size(), 1);

            // ------------------------------------------------------------------------------------------------------------
            // Test that a client can connect now
            // ------------------------------------------------------------------------------------------------------------
            channelF = nettyClient.connect(new InetSocketAddress("localhost", port));
            while (!channelF.isDone()) /** do nothing */ ;
            assertTrue(channelF.isSuccess());
            assertTrue(channelF.channel().isActive());
            // Eventually the channel group of the server should have 2 elements
            while (channelHandler.allChannels.size() != 2) /** do nothing */ ;

            // ------------------------------------------------------------------------------------------------------------
            // Close the channel on the client side and test we have one element less in the channel group
            // ------------------------------------------------------------------------------------------------------------
            channelF.channel().close().await();
            // Eventually the channel group of the server should have only one element
            while (channelHandler.allChannels.size() != 1) /** do nothing */ ;

            // ------------------------------------------------------------------------------------------------------------
            // Open a new channel and test the connection closing on the server side through the channel handler
            // ------------------------------------------------------------------------------------------------------------
            channelF = nettyClient.connect(new InetSocketAddress("localhost", port));
            while (!channelF.isDone()) /** do nothing */ ;
            assertTrue(channelF.isSuccess());
            // Eventually the channel group of the server should have 2 elements again
            while (channelHandler.allChannels.size() != 2) /** do nothing */ ;
            channelHandler.closeConnection();
            assertFalse(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 0);
            // Wait some time and check the channel was closed
            TimeUnit.SECONDS.sleep(1);
            assertFalse(channelF.channel().isOpen());

            // ------------------------------------------------------------------------------------------------------------
            // Test server re-connections with connected clients
            // ------------------------------------------------------------------------------------------------------------
            // Connect first time
            channelHandler.reconnect();
            assertTrue(channelHandler.listeningChannel.isOpen());
            // Eventually the channel group of the server should contain the listening channel
            assertEquals(channelHandler.allChannels.size(), 1);
            // Check the client can connect
            channelF = nettyClient.connect(new InetSocketAddress("localhost", port));
            while (!channelF.isDone()) /** do nothing */ ;
            assertTrue(channelF.isSuccess());
            // Eventually the channel group of the server should have 2 elements
            while (channelHandler.allChannels.size() != 2) /** do nothing */ ;
            // Re-connect and check that client connection was gone
            channelHandler.reconnect();
            assertTrue(channelHandler.listeningChannel.isOpen());
            // Eventually the channel group of the server should contain the listening channel
            assertEquals(channelHandler.allChannels.size(), 1);
            // Wait some time and check the channel was closed
            TimeUnit.SECONDS.sleep(1);
            assertFalse(channelF.channel().isOpen());

            // ------------------------------------------------------------------------------------------------------------
            // Test closeable interface with re-connection trial
            // ------------------------------------------------------------------------------------------------------------
            channelHandler.close();
            assertFalse(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.allChannels.size(), 0);
        } finally {
            if (channelHandler != null) channelHandler.close();
        }

    }

    @Test(timeOut = 10_000)
    public void testNettyChannelWriting() throws Exception {
        int port = NetworkUtils.getFreePort();
        TSOChannelHandler channelHandler = getTSOChannelHandler(port);
        try {
            // ------------------------------------------------------------------------------------------------------------
            // Prepare test
            // ------------------------------------------------------------------------------------------------------------

            // Connect channel handler
            channelHandler.reconnect();
            // Create client and connect it
            Bootstrap nettyClient = createNettyClientBootstrap();
            ChannelFuture channelF = nettyClient.connect(new InetSocketAddress("localhost", port));
            // Basic checks for connection
            while (!channelF.isDone()) /** do nothing */ ;
            assertTrue(channelF.isSuccess());
            assertTrue(channelF.channel().isActive());
            Channel channel = channelF.channel();
            // Eventually the channel group of the server should have 2 elements
            while (channelHandler.allChannels.size() != 2) /** do nothing */ ;
            // Write first handshake request
            TSOProto.HandshakeRequest.Builder handshake = TSOProto.HandshakeRequest.newBuilder();
            // NOTE: Add here the required handshake capabilities when necessary
            handshake.setClientCapabilities(TSOProto.Capabilities.newBuilder().build());
            channelF.channel().writeAndFlush(TSOProto.Request.newBuilder().setHandshakeRequest(handshake.build()).build());

            // ------------------------------------------------------------------------------------------------------------
            // Test channel writing
            // ------------------------------------------------------------------------------------------------------------
            testWritingTimestampRequest(channel);

            testWritingCommitRequest(channel);

            testWritingFenceRequest(channel);
        } finally {
            if(channelHandler != null) channelHandler.close();
        }

    }

    private void testWritingTimestampRequest(Channel channel) throws InterruptedException {
        // Reset mock
        reset(requestProcessor);
        TSOProto.Request.Builder tsBuilder = TSOProto.Request.newBuilder();
        TSOProto.TimestampRequest.Builder tsRequestBuilder = TSOProto.TimestampRequest.newBuilder();
        tsBuilder.setTimestampRequest(tsRequestBuilder.build());
        // Write into the channel
        channel.writeAndFlush(tsBuilder.build()).await();
        verify(requestProcessor, timeout(100).times(1)).timestampRequest(any(), any(MonitoringContext.class));
        verify(requestProcessor, timeout(100).times(0))
                .commitRequest(anyLong(), anyCollection(), anyCollection(), anyBoolean(), any(), any(MonitoringContext.class));
    }

    private void testWritingCommitRequest(Channel channel) throws InterruptedException {
        // Reset mock
        reset(requestProcessor);
        TSOProto.Request.Builder commitBuilder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitRequestBuilder = TSOProto.CommitRequest.newBuilder();
        commitRequestBuilder.setStartTimestamp(666);
        commitRequestBuilder.addCellId(666);
        commitBuilder.setCommitRequest(commitRequestBuilder.build());
        TSOProto.Request r = commitBuilder.build();
        assertTrue(r.hasCommitRequest());
        // Write into the channel
        channel.writeAndFlush(commitBuilder.build()).await();
        verify(requestProcessor, timeout(100).times(0)).timestampRequest(any(), any(MonitoringContext.class));
        verify(requestProcessor, timeout(100).times(1))
                .commitRequest(eq(666L), anyCollection(), anyCollection(), eq(false), any(), any(MonitoringContext.class));
    }

    private void testWritingFenceRequest(Channel channel) throws InterruptedException {
        // Reset mock
        reset(requestProcessor);
        TSOProto.Request.Builder fenceBuilder = TSOProto.Request.newBuilder();
        TSOProto.FenceRequest.Builder fenceRequestBuilder = TSOProto.FenceRequest.newBuilder();
        fenceRequestBuilder.setTableId(666);
        fenceBuilder.setFenceRequest(fenceRequestBuilder.build());
        TSOProto.Request r = fenceBuilder.build();
        assertTrue(r.hasFenceRequest());
        // Write into the channel
        channel.writeAndFlush(fenceBuilder.build()).await();
        verify(requestProcessor, timeout(100).times(0)).timestampRequest(any(), any(MonitoringContext.class));
        verify(requestProcessor, timeout(100).times(1))
                .fenceRequest(eq(666L), any(), any(MonitoringContext.class));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private Bootstrap createNettyClientBootstrap() {

        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoclient-worker-%d").build();
        EventLoopGroup workerGroup = new NioEventLoopGroup(1, workerThreadFactory);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100);

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
                pipeline.addLast("testhandler", new ChannelInboundHandlerAdapter() {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) {
                        LOG.info("Channel {} active", ctx.channel());
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        LOG.error("Error on channel {}", ctx.channel(), cause);
                    }

                });
            }
        });

        return bootstrap;
    }
}
