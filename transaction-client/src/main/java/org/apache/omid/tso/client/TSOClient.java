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

import org.apache.phoenix.thirdparty.com.google.common.base.Charsets;
import org.apache.phoenix.thirdparty.com.google.common.net.HostAndPort;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.AbstractFuture;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.omid.proto.TSOProto;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.apache.omid.zk.ZKUtils;
import org.apache.statemachine.StateMachine;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 * Describes the abstract methods to communicate to the TSO server
 */
public class TSOClient implements TSOProtocol, NodeCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(TSOClient.class);

    // Basic configuration constants & defaults TODO: Move DEFAULT_ZK_CLUSTER to a conf class???
    public static final String DEFAULT_ZK_CLUSTER = "localhost:2181";

    private static final long DEFAULT_EPOCH = -1L;
    private volatile long epoch = DEFAULT_EPOCH;

    // Attributes
    private CuratorFramework zkClient;
    private NodeCache currentTSOZNode;

    private Bootstrap bootstrap;
    private Channel currentChannel;
    private final ScheduledExecutorService fsmExecutor;
    StateMachine.Fsm fsm;

    private final int requestTimeoutInMs;
    private final int requestMaxRetries;
    private final int tsoReconnectionDelayInSecs;
    private InetSocketAddress tsoAddr;
    private String zkCurrentTsoPath;

    private boolean lowLatency;

    // Use to extract unique table identifiers from the modified cells list.

    // Conflict detection level of the entire system. Can either be Row or Cell level.
    private ConflictDetectionLevel conflictDetectionLevel;


    // ----------------------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------------------

    public static TSOClient newInstance(OmidClientConfiguration tsoClientConf) throws IOException {
        return new TSOClient(tsoClientConf);
    }

    // Avoid instantiation
    private TSOClient(OmidClientConfiguration omidConf) throws IOException {

        requestTimeoutInMs = omidConf.getRequestTimeoutInMs();
        requestMaxRetries = omidConf.getRequestMaxRetries();
        tsoReconnectionDelayInSecs = omidConf.getReconnectionDelayInSecs();

        LOG.info("Connecting to TSO...");
        HostAndPort hp;
        switch (omidConf.getConnectionType()) {
            case HA:
                zkClient = ZKUtils.initZKClient(omidConf.getConnectionString(),
                                                omidConf.getZkNamespace(),
                                                omidConf.getZkConnectionTimeoutInSecs());
                zkCurrentTsoPath = omidConf.getZkCurrentTsoPath();
                configureCurrentTSOServerZNodeCache(zkCurrentTsoPath);
                String tsoInfo = getCurrentTSOInfoFoundInZK(zkCurrentTsoPath);
                // TSO info includes the new TSO host:port address and epoch
                String[] currentTSOAndEpochArray = tsoInfo.split("#");
                hp = HostAndPort.fromString(currentTSOAndEpochArray[0]);
                setTSOAddress(hp.getHost(), hp.getPort());
                epoch = Long.parseLong(currentTSOAndEpochArray[1]);
                LOG.info("\t* Current TSO host:port found in ZK: {} Epoch {}", hp, getEpoch());
                break;
            case DIRECT:
            default:
                hp = HostAndPort.fromString(omidConf.getConnectionString());
                setTSOAddress(hp.getHost(), hp.getPort());
                LOG.info("\t* TSO host:port {} will be connected directly", hp);
                break;
        }

        fsmExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("tsofsm-%d").build());
        fsm = new StateMachine.FsmImpl(fsmExecutor);
        fsm.setInitState(new DisconnectedState(fsm));

        // Start client with the configured thread count
        int tsoExecutorThreads = omidConf.getExecutorThreads();
        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("tsoclient-worker-%d").build();
        EventLoopGroup workerGroup = new NioEventLoopGroup(tsoExecutorThreads, workerThreadFactory);

        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(8 * 1024, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("protobufdecoder", new ProtobufDecoder(TSOProto.Response.getDefaultInstance()));
                pipeline.addLast("protobufencoder", new ProtobufEncoder());
                pipeline.addLast("inboundHandler", new Handler(fsm));
            }
        });
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100);

        lowLatency = false;
        conflictDetectionLevel = omidConf.getConflictAnalysisLevel();

    }

    // ----------------------------------------------------------------------------------------------------------------
    // TSOProtocol interface
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * @see TSOProtocol#getNewStartTimestamp()
     */
    @Override
    public TSOFuture<Long> getNewStartTimestamp() {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.TimestampRequest.Builder tsreqBuilder = TSOProto.TimestampRequest.newBuilder();
        builder.setTimestampRequest(tsreqBuilder.build());
        RequestEvent request = new RequestEvent(builder.build(), requestMaxRetries);
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<>(request);
    }

    /**
     * @see TSOProtocol#commit(long, Set)
     */
    @Override
    public TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells) {
        return commit(transactionId, cells, new HashSet<CellId>());
    }

    /**
     * @see TSOProtocol#commit(long, Set, Set)
     */
    @Override
    public TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells, Set<? extends CellId> conflictFreeWriteSet) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitbuilder = TSOProto.CommitRequest.newBuilder();
        commitbuilder.setStartTimestamp(transactionId);
        HashSet<Long> rowLevelWriteSet = new HashSet<Long>();
        HashSet<Long> tableIDs = new HashSet<Long>();
        rowLevelWriteSet.clear();
        for (CellId cell : cells) {
            long id;

            switch (conflictDetectionLevel) {
            case ROW:
                id = cell.getRowId();
                if (rowLevelWriteSet.contains(id)) {
                    continue;
                } else {
                    rowLevelWriteSet.add(id);
                }
                break;
            case CELL:
                id = cell.getCellId();
                break;
            default:
                id = 0;
                assert (false);
            }

            commitbuilder.addCellId(id);
            tableIDs.add(cell.getTableId());
        }

        for (CellId cell : conflictFreeWriteSet) {
            tableIDs.add(cell.getTableId());
        }

        commitbuilder.addAllTableId(tableIDs);
        tableIDs.clear();
        builder.setCommitRequest(commitbuilder.build());
        RequestEvent request = new RequestEvent(builder.build(), requestMaxRetries);
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<>(request);
    }

    /**
     * @see TSOProtocol#getFence(long)
     */
    @Override
    public TSOFuture<Long> getFence(long tableId) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.FenceRequest.Builder fenceReqBuilder = TSOProto.FenceRequest.newBuilder();
        fenceReqBuilder.setTableId(tableId);
        builder.setFenceRequest(fenceReqBuilder.build());
        RequestEvent request = new RequestEvent(builder.build(), requestMaxRetries);
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<>(request);
    }

    /**
     * @see TSOProtocol#close()
     */
    @Override
    public TSOFuture<Void> close() {
        final CloseEvent closeEvent = new CloseEvent();
        fsm.sendEvent(closeEvent);
        closeEvent.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    closeEvent.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    fsmExecutor.shutdown();
                    if (currentTSOZNode != null) {
                        try {
                            currentTSOZNode.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (zkClient != null) {
                        zkClient.close();
                    }
                }

            }
        }, fsmExecutor);
        return new ForwardingTSOFuture<>(closeEvent);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // High availability related interface
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * @see TSOProtocol#getEpoch()
     */
    @Override
    public long getEpoch() {
        return epoch;
    }

    /**
     * Used for family deletion
     * @return the conflict detection level.
     */
    @Override
    public ConflictDetectionLevel getConflictDetectionLevel() {
        return conflictDetectionLevel;
    }

    /**
     * Used for family deletion testing
     */
    @Override
    public void setConflictDetectionLevel(ConflictDetectionLevel conflictDetectionLevel) {
        this.conflictDetectionLevel = conflictDetectionLevel;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // NodeCacheListener interface
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void nodeChanged() throws Exception {

        String tsoInfo = getCurrentTSOInfoFoundInZK(zkCurrentTsoPath);
        // TSO info includes the new TSO host:port address and epoch
        String[] currentTSOAndEpochArray = tsoInfo.split("#");
        HostAndPort hp = HostAndPort.fromString(currentTSOAndEpochArray[0]);
        setTSOAddress(hp.getHost(), hp.getPort());
        epoch = Long.parseLong(currentTSOAndEpochArray[1]);
        LOG.info("CurrentTSO ZNode changed. New TSO Host & Port {}/Epoch {}", hp, getEpoch());
        if (currentChannel != null && currentChannel.isActive()) {
            LOG.info("\tClosing channel with previous TSO {}", currentChannel);
            currentChannel.close();
        }

    }

    @Override
    public boolean isLowLatency() {
        return lowLatency;
    }

    // ****************************************** Finite State Machine ************************************************

    // ----------------------------------------------------------------------------------------------------------------
    // FSM: Events
    // ----------------------------------------------------------------------------------------------------------------

    private static class ParamEvent<T> implements StateMachine.Event {

        final T param;

        ParamEvent(T param) {
            this.param = param;
        }

        T getParam() {
            return param;
        }
    }

    private static class ErrorEvent extends ParamEvent<Throwable> {

        ErrorEvent(Throwable t) {
            super(t);
        }
    }

    private static class ConnectedEvent extends ParamEvent<Channel> {

        ConnectedEvent(Channel c) {
            super(c);
        }
    }

    private static class UserEvent<T> extends AbstractFuture<T>
            implements StateMachine.DeferrableEvent {

        void success(T value) {
            set(value);
        }

        @Override
        public void error(Throwable t) {
            setException(t);
        }
    }

    private static class CloseEvent extends UserEvent<Void> {

    }

    private static class ChannelClosedEvent extends ParamEvent<Throwable> {

        ChannelClosedEvent(Throwable t) {
            super(t);
        }
    }

    private static class ReconnectEvent implements StateMachine.Event {

    }

    private static class HandshakeTimeoutEvent implements StateMachine.Event {

    }

    private static class TimestampRequestTimeoutEvent implements StateMachine.Event {

    }

    private static class CommitRequestTimeoutEvent implements StateMachine.Event {

        final long startTimestamp;

        CommitRequestTimeoutEvent(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }
    }

    private static class FenceRequestTimeoutEvent implements StateMachine.Event {

        final long tableID;

        FenceRequestTimeoutEvent(long tableID) {
            this.tableID = tableID;
        }

        public long getTableID() {
            return tableID;
        }
    }

    private static class RequestEvent extends UserEvent<Long> {

        TSOProto.Request req;
        int retriesLeft;

        RequestEvent(TSOProto.Request req, int retriesLeft) {
            this.req = req;
            this.retriesLeft = retriesLeft;
        }

        TSOProto.Request getRequest() {
            return req;
        }

        void setRequest(TSOProto.Request request) {
            this.req = request;
        }

        int getRetriesLeft() {
            return retriesLeft;
        }

        void decrementRetries() {
            retriesLeft--;
        }

    }

    private static class ResponseEvent extends ParamEvent<TSOProto.Response> {

        ResponseEvent(TSOProto.Response r) {
            super(r);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // FSM: States
    // ----------------------------------------------------------------------------------------------------------------

    class BaseState extends StateMachine.State {

        BaseState(StateMachine.Fsm fsm) {
            super(fsm);
        }

        public StateMachine.State handleEvent(StateMachine.Event e) {
            LOG.error("Unhandled event {} while in state {}", e, this.getClass().getName());
            return this;
        }
    }

    class DisconnectedState extends BaseState {

        DisconnectedState(StateMachine.Fsm fsm) {
            super(fsm);
            LOG.debug("NEW STATE: DISCONNECTED");
        }

        public StateMachine.State handleEvent(RequestEvent e) {
            fsm.deferEvent(e);
            return tryToConnectToTSOServer();
        }

        public StateMachine.State handleEvent(CloseEvent e) {
            bootstrap.config().group().shutdownGracefully();
            e.success(null);
            return this;
        }

        private StateMachine.State tryToConnectToTSOServer() {
            final InetSocketAddress tsoAddress = getAddress();
            LOG.info("Trying to connect to TSO [{}]", tsoAddress);
            ChannelFuture channelFuture = bootstrap.connect(tsoAddress);
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        LOG.info("Connection to TSO [{}] established. Channel {}",
                                 tsoAddress, channelFuture.channel());
                    } else {
                        LOG.error("Failed connection attempt to TSO [{}] failed. Channel {}",
                                  tsoAddress, channelFuture.channel());
                        fsm.sendEvent(new ErrorEvent(new ConnectionException()));
                    }
                }
            });
            return new ConnectingState(fsm);
        }
    }

    private class ConnectingState extends BaseState {

        ConnectingState(StateMachine.Fsm fsm) {
            super(fsm);
            LOG.debug("NEW STATE: CONNECTING");
        }

        public StateMachine.State handleEvent(UserEvent e) {
            fsm.deferEvent(e);
            return this;
        }

        public StateMachine.State handleEvent(ConnectedEvent e) {
            return new HandshakingState(fsm, e.getParam());
        }

        public StateMachine.State handleEvent(ChannelClosedEvent e) {
            return new ConnectionFailedState(fsm, e.getParam());
        }

        public StateMachine.State handleEvent(ErrorEvent e) {
            return new ConnectionFailedState(fsm, e.getParam());
        }

    }

    private static class RequestAndTimeout {

        final RequestEvent event;
        final Timeout timeout;

        RequestAndTimeout(RequestEvent event, Timeout timeout) {
            this.event = event;
            this.timeout = timeout;
        }

        RequestEvent getRequest() {
            return event;
        }

        Timeout getTimeout() {
            return timeout;
        }

        @Override
        public String toString() {
            String info = "Request type ";
            if (event.getRequest().hasTimestampRequest()) {
                info += "[Timestamp]";
            } else if (event.getRequest().hasCommitRequest()) {
                info += "[Commit] Start TS ->" + event.getRequest().getCommitRequest().getStartTimestamp();
            } else {
                info += "NONE";
            }
            return info;
        }
    }

    private class HandshakingState extends BaseState {

        final Channel channel;

        final HashedWheelTimer timeoutExecutor = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("tso-client-timeout").build());
        final Timeout timeout;

        HandshakingState(StateMachine.Fsm fsm, Channel channel) {
            super(fsm);
            LOG.debug("NEW STATE: HANDSHAKING");
            this.channel = channel;
            TSOProto.HandshakeRequest.Builder handshake = TSOProto.HandshakeRequest.newBuilder();
            // Add the required handshake capabilities when necessary
            handshake.setClientCapabilities(TSOProto.Capabilities.newBuilder().build());
            channel.writeAndFlush(TSOProto.Request.newBuilder().setHandshakeRequest(handshake.build()).build());
            timeout = newTimeout();
        }

        private Timeout newTimeout() {
            if (requestTimeoutInMs > 0) {
                return timeoutExecutor.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        fsm.sendEvent(new HandshakeTimeoutEvent());
                    }
                }, 30, TimeUnit.SECONDS);
            } else {
                return null;
            }
        }

        public StateMachine.State handleEvent(UserEvent e) {
            fsm.deferEvent(e);
            return this;
        }

        public StateMachine.State handleEvent(ResponseEvent e) {
            lowLatency = e.getParam().getHandshakeResponse().getLowLatency();
            if (e.getParam().hasHandshakeResponse() && e.getParam().getHandshakeResponse().getClientCompatible()) {
                if (timeout != null) {
                    timeout.cancel();
                }
                return new ConnectedState(fsm, channel, timeoutExecutor);
            } else {
                cleanupState();
                LOG.error("Client incompatible with server");
                return new HandshakeFailedState(fsm, new HandshakeFailedException());
            }
        }

        public StateMachine.State handleEvent(HandshakeTimeoutEvent e) {
            cleanupState();
            return new ClosingState(fsm);
        }

        public StateMachine.State handleEvent(ErrorEvent e) {
            cleanupState();
            Throwable exception = e.getParam();
            LOG.error("Error during handshake", exception);
            return new HandshakeFailedState(fsm, exception);
        }

        private void cleanupState() {
            timeoutExecutor.stop();
            channel.close();
            if (timeout != null) {
                timeout.cancel();
            }
        }

    }

    class ConnectionFailedState extends BaseState {

        final HashedWheelTimer reconnectionTimeoutExecutor = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("tso-client-backoff-timeout").build());

        Throwable exception;
        Timeout timeout;

        ConnectionFailedState(final StateMachine.Fsm fsm, final Throwable exception) {
            super(fsm);
            LOG.debug("NEW STATE: CONNECTION FAILED [RE-CONNECTION BACKOFF]");
            this.exception = exception;
            this.timeout = reconnectionTimeoutExecutor.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) {
                    fsm.sendEvent(new ReconnectEvent());
                }
            }, tsoReconnectionDelayInSecs, TimeUnit.SECONDS);
        }

        public StateMachine.State handleEvent(UserEvent e) {
            e.error(exception);
            return this;
        }

        public StateMachine.State handleEvent(ErrorEvent e) {
            return this;
        }

        public StateMachine.State handleEvent(ChannelClosedEvent e) {
            cleanupState();
            return new DisconnectedState(fsm);
        }

        public StateMachine.State handleEvent(ReconnectEvent e){
            cleanupState();
            return new DisconnectedState(fsm);
        }

        private void cleanupState() {
            if(timeout != null) timeout.cancel();
            reconnectionTimeoutExecutor.stop();
        }
    }

    private class HandshakeFailedState extends ConnectionFailedState {

        HandshakeFailedState(StateMachine.Fsm fsm, Throwable exception) {
            super(fsm, exception);
            LOG.debug("STATE: HANDSHAKING FAILED");
        }

    }

    class ConnectedState extends BaseState {

        final Queue<RequestAndTimeout> timestampRequests;
        final Map<Long, RequestAndTimeout> commitRequests;
        final Map<Long, RequestAndTimeout> fenceRequests;
        final Channel channel;

        final HashedWheelTimer timeoutExecutor;

        ConnectedState(StateMachine.Fsm fsm, Channel channel, HashedWheelTimer timeoutExecutor) {
            super(fsm);
            LOG.debug("NEW STATE: CONNECTED");
            this.channel = channel;
            this.timeoutExecutor = timeoutExecutor;
            timestampRequests = new ArrayDeque<>();
            commitRequests = new HashMap<>();
            fenceRequests = new HashMap<>();
        }

        private Timeout newTimeout(final StateMachine.Event timeoutEvent) {
            if (requestTimeoutInMs > 0) {
                return timeoutExecutor.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        fsm.sendEvent(timeoutEvent);
                    }
                }, requestTimeoutInMs, TimeUnit.MILLISECONDS);
            } else {
                return null;
            }
        }

        private void sendRequest(final StateMachine.Fsm fsm, RequestEvent request) {
            TSOProto.Request req = request.getRequest();

            if (req.hasTimestampRequest()) {
                timestampRequests.add(new RequestAndTimeout(request, newTimeout(new TimestampRequestTimeoutEvent())));
            } else if (req.hasCommitRequest()) {
                TSOProto.CommitRequest commitReq = req.getCommitRequest();
                commitRequests.put(commitReq.getStartTimestamp(), new RequestAndTimeout(
                        request, newTimeout(new CommitRequestTimeoutEvent(commitReq.getStartTimestamp()))));
            } else if (req.hasFenceRequest()) {
                TSOProto.FenceRequest fenceReq = req.getFenceRequest();
                fenceRequests.put(fenceReq.getTableId(), new RequestAndTimeout(
                        request, newTimeout(new FenceRequestTimeoutEvent(fenceReq.getTableId()))));
            } else {
                request.error(new IllegalArgumentException("Unknown request type"));
                return;
            }
            ChannelFuture f = channel.writeAndFlush(req);

            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (!future.isSuccess()) {
                        fsm.sendEvent(new ErrorEvent(future.cause()));
                    }
                }
            });
        }

        private void handleResponse(ResponseEvent response) {
            TSOProto.Response resp = response.getParam();
            if (resp.hasTimestampResponse()) {
                if (timestampRequests.size() == 0) {
                    LOG.debug("Received timestamp response when no requests outstanding");
                    return;
                }
                RequestAndTimeout e = timestampRequests.remove();
                e.getRequest().success(resp.getTimestampResponse().getStartTimestamp());
                if (e.getTimeout() != null) {
                    e.getTimeout().cancel();
                }
            } else if (resp.hasCommitResponse()) {
                long startTimestamp = resp.getCommitResponse().getStartTimestamp();
                RequestAndTimeout e = commitRequests.remove(startTimestamp);
                if (e == null) {
                    LOG.debug("Received commit response for request that doesn't exist. Start TS: {}", startTimestamp);
                    return;
                }
                if (e.getTimeout() != null) {
                    e.getTimeout().cancel();
                }
                if (resp.getCommitResponse().getAborted()) {
                    e.getRequest().error(new AbortException());
                } else {
                    e.getRequest().success(resp.getCommitResponse().getCommitTimestamp());
                }
            } else if (resp.hasFenceResponse()) {
                long tableID = resp.getFenceResponse().getTableId();
                RequestAndTimeout e = fenceRequests.remove(tableID);
                if (e == null) {
                    LOG.debug("Received fence response for request that doesn't exist. Table ID: {}", tableID);
                    return;
                }
                if (e.getTimeout() != null) {
                    e.getTimeout().cancel();
                }

                e.getRequest().success(resp.getFenceResponse().getFenceId());
            }
        }

        public StateMachine.State handleEvent(TimestampRequestTimeoutEvent e) {
            if (!timestampRequests.isEmpty()) {
                RequestAndTimeout r = timestampRequests.remove();
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                queueRetryOrError(fsm, r.getRequest());
            }
            return this;
        }

        public StateMachine.State handleEvent(CommitRequestTimeoutEvent e) {
            long startTimestamp = e.getStartTimestamp();
            if (commitRequests.containsKey(startTimestamp)) {
                RequestAndTimeout r = commitRequests.remove(startTimestamp);
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                queueRetryOrError(fsm, r.getRequest());
            }
            return this;
        }

        public StateMachine.State handleEvent(FenceRequestTimeoutEvent e) {
            long tableID = e.getTableID();
            if (fenceRequests.containsKey(tableID)) {
                RequestAndTimeout r = fenceRequests.remove(tableID);
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                queueRetryOrError(fsm, r.getRequest());
            }
            return this;
        }

        public StateMachine.State handleEvent(CloseEvent e) {
            LOG.debug("CONNECTED STATE: CloseEvent");
            timeoutExecutor.stop();
            closeChannelAndErrorRequests();
            fsm.deferEvent(e);
            return new ClosingState(fsm);
        }

        public StateMachine.State handleEvent(RequestEvent e) {
            sendRequest(fsm, e);
            return this;
        }

        public StateMachine.State handleEvent(ResponseEvent e) {
            handleResponse(e);
            return this;
        }

        public StateMachine.State handleEvent(ErrorEvent e) {
            LOG.debug("CONNECTED STATE: ErrorEvent");
            timeoutExecutor.stop();
            handleError(fsm);
            return new ClosingState(fsm);
        }

        private void handleError(StateMachine.Fsm fsm) {
            LOG.debug("CONNECTED STATE: Cancelling Timeouts in handleError");
            while (timestampRequests.size() > 0) {
                RequestAndTimeout r = timestampRequests.remove();
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                queueRetryOrError(fsm, r.getRequest());
            }
            Iterator<Map.Entry<Long, RequestAndTimeout>> iter = commitRequests.entrySet().iterator();
            while (iter.hasNext()) {
                RequestAndTimeout r = iter.next().getValue();
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                queueRetryOrError(fsm, r.getRequest());
                iter.remove();
            }
            iter = fenceRequests.entrySet().iterator();
            while (iter.hasNext()) {
                RequestAndTimeout r = iter.next().getValue();
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                queueRetryOrError(fsm, r.getRequest());
                iter.remove();
            }
            channel.close();
        }

        private void queueRetryOrError(StateMachine.Fsm fsm, RequestEvent e) {
            if (e.getRetriesLeft() > 0) {
                e.decrementRetries();
                if (e.getRequest().hasCommitRequest()) {
                    TSOProto.CommitRequest commitRequest = e.getRequest().getCommitRequest();
                    if (!commitRequest.getIsRetry()) { // Create a new retry for the commit request
                        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
                        TSOProto.CommitRequest.Builder commitBuilder = TSOProto.CommitRequest.newBuilder();
                        commitBuilder.mergeFrom(commitRequest);
                        commitBuilder.setIsRetry(true);
                        builder.setCommitRequest(commitBuilder.build());
                        e.setRequest(builder.build());
                    }
                }
                fsm.sendEvent(e);
            } else {
                e.error(
                        new ServiceUnavailableException("Number of retries exceeded. This API request failed permanently"));
            }
        }

        private void closeChannelAndErrorRequests() {
            channel.close();
            for (RequestAndTimeout r : timestampRequests) {
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                r.getRequest().error(new ClosingException());
            }
            for (RequestAndTimeout r : commitRequests.values()) {
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                r.getRequest().error(new ClosingException());
            }
            for (RequestAndTimeout r : fenceRequests.values()) {
                if (r.getTimeout() != null) {
                    r.getTimeout().cancel();
                }
                r.getRequest().error(new ClosingException());
            }
        }
    }

    private class ClosingState extends BaseState {

        ClosingState(StateMachine.Fsm fsm) {
            super(fsm);
            LOG.debug("NEW STATE: CLOSING");
        }

        public StateMachine.State handleEvent(TimestampRequestTimeoutEvent e) {
            // Ignored. They will be retried or errored
            return this;
        }

        public StateMachine.State handleEvent(CommitRequestTimeoutEvent e) {
            // Ignored. They will be retried or errored
            return this;
        }

        public StateMachine.State handleEvent(FenceRequestTimeoutEvent e) {
            // Ignored. They will be retried or errored
            return this;
        }

        public StateMachine.State handleEvent(ErrorEvent e) {
            // Ignored. They will be retried or errored
            return this;
        }

        public StateMachine.State handleEvent(ResponseEvent e) {
            // Ignored. They will be retried or errored
            return this;
        }

        public StateMachine.State handleEvent(UserEvent e) {
            fsm.deferEvent(e);
            return this;
        }

        public StateMachine.State handleEvent(ChannelClosedEvent e) {
            return new DisconnectedState(fsm);
        }

        public StateMachine.State handleEvent(HandshakeTimeoutEvent e) {
            return this;
        }

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper classes & methods
    // ----------------------------------------------------------------------------------------------------------------

    private class Handler extends ChannelInboundHandlerAdapter {

        private StateMachine.Fsm fsm;

        Handler(StateMachine.Fsm fsm) {
            this.fsm = fsm;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            currentChannel = ctx.channel();
            LOG.debug("HANDLER (CHANNEL ACTIVE): Connection {}. Sending connected event to FSM", ctx.channel());
            fsm.sendEvent(new ConnectedEvent(ctx.channel()));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.debug("HANDLER (CHANNEL INACTIVE): Connection {}. Sending error, then channelClosed event to FSM", ctx.channel());
            // Netty 3 had separate callbacks, and the FSM expects both events.
            // Sending both is much easier than rewriting the FSM
            fsm.sendEvent(new ErrorEvent(new ConnectionException()));
            fsm.sendEvent(new ChannelClosedEvent(new ConnectionException()));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof TSOProto.Response) {
                fsm.sendEvent(new ResponseEvent((TSOProto.Response) msg));
            } else {
                LOG.warn("Received unknown message", msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.error("Error on channel {}", ctx.channel(), cause);
            fsm.sendEvent(new ErrorEvent(cause));
        }
    }

    private synchronized void setTSOAddress(String host, int port) {
        tsoAddr = new InetSocketAddress(host, port);
    }

    private synchronized InetSocketAddress getAddress() {
        return tsoAddr;
    }

    private void configureCurrentTSOServerZNodeCache(String currentTsoPath) {
        try {
            currentTSOZNode = new NodeCache(zkClient, currentTsoPath);
            currentTSOZNode.getListenable().addListener(this);
            currentTSOZNode.start(true);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot start watcher on current TSO Server ZNode: " + e.getMessage());
        }
    }

    private String getCurrentTSOInfoFoundInZK(String currentTsoPath) {
        ChildData currentTSOData = currentTSOZNode.getCurrentData();
        if (currentTSOData == null) {
            throw new IllegalStateException("No data found in ZKNode " + currentTsoPath);
        }
        byte[] currentTSOAndEpochAsBytes = currentTSOData.getData();
        if (currentTSOAndEpochAsBytes == null) {
            throw new IllegalStateException("No data found for current TSO in ZKNode " + currentTsoPath);
        }
        return new String(currentTSOAndEpochAsBytes, Charsets.UTF_8);
    }

}
