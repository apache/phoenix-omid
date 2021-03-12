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

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.dsl.Disruptor;

import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.tso.TSOStateManager.TSOState;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.lmax.disruptor.dsl.ProducerType.MULTI;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.omid.tso.AbstractRequestProcessor.RequestEvent.EVENT_FACTORY;

abstract class AbstractRequestProcessor implements EventHandler<AbstractRequestProcessor.RequestEvent>, RequestProcessor, TimeoutHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRequestProcessor.class);

    // Disruptor-related attributes
    private final ExecutorService disruptorExec;
    protected final Disruptor<RequestEvent> disruptor;
    protected RingBuffer<RequestEvent> requestRing;

    private final TimestampOracle timestampOracle;
    private final CommitHashMap hashmap;
    private final Map<Long, Long> tableFences;
    private final MetricsRegistry metrics;
    private final LowWatermarkWriter lowWatermarkWriter;
    private long lowWatermark = -1L;

    //Used to forward fence
    private final ReplyProcessor replyProcessor;

    AbstractRequestProcessor(MetricsRegistry metrics,
                             TimestampOracle timestampOracle,
                             Panicker panicker,
                             TSOServerConfig config,
                             LowWatermarkWriter lowWatermarkWriter, ReplyProcessor replyProcessor)
            throws IOException {


        // ------------------------------------------------------------------------------------------------------------
        // Disruptor initialization
        // ------------------------------------------------------------------------------------------------------------

        TimeoutBlockingWaitStrategy timeoutStrategy = new TimeoutBlockingWaitStrategy(config.getBatchPersistTimeoutInMs(), MILLISECONDS);

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("request-%d").build();
        this.disruptorExec = Executors.newSingleThreadExecutor(threadFactory);

        this.disruptor = new Disruptor<>(EVENT_FACTORY, 1 << 12, disruptorExec, MULTI, timeoutStrategy);
        disruptor.handleExceptionsWith(new FatalExceptionHandler(panicker)); // This must be before handleEventsWith()
        disruptor.handleEventsWith(this);


        // ------------------------------------------------------------------------------------------------------------
        // Attribute initialization
        // ------------------------------------------------------------------------------------------------------------

        this.metrics = metrics;
        this.timestampOracle = timestampOracle;
        this.hashmap = new CommitHashMap(config.getConflictMapSize());
        this.tableFences = new HashMap<Long, Long>();
        this.lowWatermarkWriter = lowWatermarkWriter;

        this.replyProcessor = replyProcessor;

        LOG.info("RequestProcessor initialized");

    }

    /**
     * This should be called when the TSO gets leadership
     */
    @Override
    public void update(TSOState state) throws Exception {
        LOG.info("Initializing RequestProcessor state...");
        this.lowWatermark = state.getLowWatermark();
        lowWatermarkWriter.persistLowWatermark(lowWatermark).get(); // Sync persist
        LOG.info("RequestProcessor state initialized with LWMs {} and Epoch {}", lowWatermark, state.getEpoch());
    }

    @Override
    public void onEvent(RequestEvent event, long sequence, boolean endOfBatch) throws Exception {

        switch (event.getType()) {
            case TIMESTAMP:
                handleTimestamp(event);
                break;
            case COMMIT:
                handleCommit(event);
                break;
            case FENCE:
                handleFence(event);
                break;
            default:
                throw new IllegalStateException("Event not allowed in Request Processor: " + event);
        }

    }

    @Override
    public void onTimeout(long sequence) throws Exception {

        // TODO We can not use this as a timeout trigger for flushing. This timeout is related to the time between
        // TODO (cont) arrivals of requests to the disruptor. We need another mechanism to trigger timeouts
        // TODO (cont) WARNING!!! Take care with the implementation because if there's other thread than request-0
        // TODO (cont) thread the one that calls persistProc.triggerCurrentBatchFlush(); we'll incur in concurrency issues
        // TODO (cont) This is because, in the current implementation, only the request-0 thread calls the public methods
        // TODO (cont) in persistProc and it is guaranteed that access them serially.
        onTimeout();
    }

    @Override
    public void timestampRequest(Channel c, MonitoringContext monCtx) {

        monCtx.timerStart("request.processor.timestamp.latency");
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeTimestampRequest(e, c, monCtx);
        requestRing.publish(seq);

    }

    @Override
    public void commitRequest(long startTimestamp, Collection<Long> writeSet, Collection<Long> tableIdSet, boolean isRetry, Channel c,
                              MonitoringContext monCtx) {

        monCtx.timerStart("request.processor.commit.latency");
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeCommitRequest(e, startTimestamp, monCtx, writeSet, tableIdSet, isRetry, c);
        requestRing.publish(seq);

    }

    @Override
    public void fenceRequest(long tableID, Channel c, MonitoringContext monCtx) {

        monCtx.timerStart("request.processor.fence.latency");
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeFenceRequest(e, tableID, c, monCtx);
        requestRing.publish(seq);

    }

    private void handleTimestamp(RequestEvent requestEvent) throws Exception {

        long timestamp = timestampOracle.next();
        requestEvent.getMonCtx().timerStop("request.processor.timestamp.latency");
        forwardTimestamp(timestamp, requestEvent.getChannel(), requestEvent.getMonCtx());
    }

    // Checks whether transaction transactionId started before a fence creation of a table transactionId modified.
    private boolean hasConflictsWithFences(long startTimestamp, Collection<Long> tableIdSet) {
        if (!tableFences.isEmpty()) {
            for (long tableId: tableIdSet) {
                Long fence = tableFences.get(tableId);
                if (fence != null && fence > startTimestamp) {
                    return true;
                }
                if (fence != null && fence < lowWatermark) {
                    tableFences.remove(tableId); // Garbage collect entries of old fences.
                }
            }
        }

        return false;
    }

 // Checks whether transactionId has a write-write conflict with a transaction committed after transactionId.
    private boolean hasConflictsWithCommittedTransactions(long startTimestamp, Iterable<Long> writeSet) {
        for (long cellId : writeSet) {
            long value = hashmap.getLatestWriteForCell(cellId);
            if (value != 0 && value >= startTimestamp) {
                return true;
            }
        }

        return false;
    }

    private void handleCommit(RequestEvent event) throws Exception {

        long startTimestamp = event.getStartTimestamp();
        Iterable<Long> writeSet = event.writeSet();
        Collection<Long> tableIdSet = event.getTableIdSet();
        boolean isCommitRetry = event.isCommitRetry();
        Channel c = event.getChannel();

        boolean nonEmptyWriteSet = writeSet.iterator().hasNext();

        // If the transaction started before the low watermark, or
        // it started before a fence and modified the table the fence created for, or
        // it has a write-write conflict with a transaction committed after it started
        // Then it should abort. Otherwise, it can commit.
        if (startTimestamp > lowWatermark &&
            !hasConflictsWithFences(startTimestamp, tableIdSet) &&
            !hasConflictsWithCommittedTransactions(startTimestamp, writeSet)) {

            long commitTimestamp = timestampOracle.next();
            Optional<Long> forwardNewWaterMark = Optional.absent();
            if (nonEmptyWriteSet) {
                long newLowWatermark = lowWatermark;

                for (long r : writeSet) {
                    long removed = hashmap.putLatestWriteForCell(r, commitTimestamp);
                    newLowWatermark = Math.max(removed, newLowWatermark);
                }

                if (newLowWatermark != lowWatermark) {
                    LOG.trace("Setting new low Watermark to {}", newLowWatermark);
                    lowWatermark = newLowWatermark;
                    forwardNewWaterMark = Optional.of(lowWatermark);
                }
            }
            event.getMonCtx().timerStop("request.processor.commit.latency");
            forwardCommit(startTimestamp, commitTimestamp, c, event.getMonCtx(), forwardNewWaterMark);

        } else {

            event.getMonCtx().timerStop("request.processor.commit.latency");
            if (isCommitRetry) { // Re-check if it was already committed but the client retried due to a lag replying
                forwardCommitRetry(startTimestamp, c, event.getMonCtx());
            } else {
                forwardAbort(startTimestamp, c, event.getMonCtx());
            }

        }

    }

    private void handleFence(RequestEvent event) throws Exception {
        long tableID = event.getTableId();
        Channel c = event.getChannel();

        long fenceTimestamp = timestampOracle.next();

        tableFences.put(tableID, fenceTimestamp);

        event.monCtx.timerStart("reply.processor.fence.latency");
        replyProcessor.sendFenceResponse(tableID, fenceTimestamp, c, event.monCtx);
    }

    @Override
    public void close() throws IOException {

        LOG.info("Terminating Request Processor...");
        disruptor.halt();
        disruptor.shutdown();
        LOG.info("\tRequest Processor Disruptor shutdown");
        disruptorExec.shutdownNow();
        try {
            disruptorExec.awaitTermination(3, SECONDS);
            LOG.info("\tRequest Processor Disruptor executor shutdown");
        } catch (InterruptedException e) {
            LOG.error("Interrupted whilst finishing Request Processor Disruptor executor");
            Thread.currentThread().interrupt();
        }
        LOG.info("Request Processor terminated");

    }

    protected abstract void forwardCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx, Optional<Long> lowWatermark) throws Exception;
    protected abstract void forwardCommitRetry(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception;
    protected abstract void forwardAbort(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception;
    protected abstract void forwardTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception;
    protected abstract void onTimeout() throws Exception;



    final static class RequestEvent implements Iterable<Long> {

        enum Type {
            TIMESTAMP, COMMIT, FENCE
        }

        private Type type = null;
        private Channel channel = null;

        private boolean isCommitRetry = false;
        private long startTimestamp = 0;
        private MonitoringContext monCtx;
        private long numCells = 0;

        private static final int MAX_INLINE = 40;
        private Long writeSet[] = new Long[MAX_INLINE];
        private Collection<Long> writeSetAsCollection = null; // for the case where there's more than MAX_INLINE

        private Collection<Long> tableIdSet = null;
        private long tableID = 0;

        static void makeTimestampRequest(RequestEvent e, Channel c, MonitoringContext monCtx) {
            e.type = Type.TIMESTAMP;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makeCommitRequest(RequestEvent e,
                                      long startTimestamp,
                                      MonitoringContext monCtx,
                                      Collection<Long> writeSet,
                                      Collection<Long> TableIdSet,
                                      boolean isRetry,
                                      Channel c) {
            e.monCtx = monCtx;
            e.type = Type.COMMIT;
            e.channel = c;
            e.startTimestamp = startTimestamp;
            e.isCommitRetry = isRetry;
            if (writeSet.size() > MAX_INLINE) {
                e.numCells = writeSet.size();
                e.writeSetAsCollection = writeSet;
            } else {
                e.writeSetAsCollection = null;
                e.numCells = writeSet.size();
                int i = 0;
                for (Long cellId : writeSet) {
                    e.writeSet[i] = cellId;
                    ++i;
                }
            }
            e.tableIdSet = TableIdSet;
        }

        static void makeFenceRequest(RequestEvent e,
                                     long tableID,
                                     Channel c,
                                     MonitoringContext monCtx) {
            e.type = Type.FENCE;
            e.channel = c;
            e.monCtx = monCtx;
            e.tableID = tableID;
        }

        MonitoringContext getMonCtx() {
            return monCtx;
        }

        Type getType() {
            return type;
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        Channel getChannel() {
            return channel;
        }

        Collection<Long> getTableIdSet() {
            return tableIdSet;
        }

        long getTableId() {
            return tableID;
        }

        @Override
        public Iterator<Long> iterator() {

            if (writeSetAsCollection != null) {
                return writeSetAsCollection.iterator();
            }

            return new Iterator<Long>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < numCells;
                }

                @Override
                public Long next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return writeSet[i++];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

        }

        Iterable<Long> writeSet() {

            return this;

        }

        boolean isCommitRetry() {
            return isCommitRetry;
        }

        final static EventFactory<RequestEvent> EVENT_FACTORY = new EventFactory<RequestEvent>() {
            @Override
            public RequestEvent newInstance() {
                return new RequestEvent();
            }
        };

    }

}
