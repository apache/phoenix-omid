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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.commons.pool2.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestBatchPool {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatchPool.class);

    private static final int CONCURRENT_WRITERS = 16;
    private static final int BATCH_SIZE = 1000;


    private Injector injector;

    @BeforeMethod
    void setup() {

        TSOServerConfig tsoServerConfig = new TSOServerConfig();
        tsoServerConfig.setNumConcurrentCTWriters(CONCURRENT_WRITERS);
        tsoServerConfig.setBatchSizePerCTWriter(BATCH_SIZE);

        // Injector to get the element under test: the ObjectPool<Batch> returned by Guice's BatchPoolModule
        injector = Guice.createInjector(new BatchPoolModule(tsoServerConfig));

    }

    @Test(timeOut = 10_000)
    public void testBatchPoolObtainedIsSingleton() {

        final ObjectPool<Batch> instance1 = injector.getInstance(Key.get(new TypeLiteral<ObjectPool<Batch>>() {}));
        final ObjectPool<Batch> instance2 = injector.getInstance(Key.get(new TypeLiteral<ObjectPool<Batch>>() {}));

        assertEquals(instance1, instance2, "Objects are NOT equal !");

    }

    @Test(timeOut = 10_000)
    public void testBatchPoolInitializesAllBatchObjectsAsIdle() throws Exception {

        final ObjectPool<Batch> batchPool = injector.getInstance(Key.get(new TypeLiteral<ObjectPool<Batch>>() {}));

        assertEquals(batchPool.getNumActive(), 0);
        assertEquals(batchPool.getNumIdle(), CONCURRENT_WRITERS);

        // Now make all of them active and check it below
        for (int i = 0; i < CONCURRENT_WRITERS; i++) {
            batchPool.borrowObject();
        }

        assertEquals(batchPool.getNumActive(), CONCURRENT_WRITERS);
        assertEquals(batchPool.getNumIdle(), 0);

    }

    @Test(timeOut = 10_000)
    public void testBatchPoolBlocksWhenAllObjectsAreActive() throws Exception {

        ExecutorService executor = Executors.newCachedThreadPool();

        final ObjectPool<Batch> batchPool = injector.getInstance(Key.get(new TypeLiteral<ObjectPool<Batch>>() {}));

        // Try to get one more batch than the number of concurrent writers set
        for (int i = 0; i < CONCURRENT_WRITERS + 1; i++) {

            // Wrap the call to batchPool.borrowObject() in a task to detect when is blocked by the ObjectPool
            Callable<Batch> task = new Callable<Batch>() {
                public Batch call() throws Exception {
                    return batchPool.borrowObject();
                }
            };

            Future<Batch> future = executor.submit(task);

            try {
                /** The future below should return immediately except for the last one, which should be blocked by
                    the ObjectPool as per the configuration setup in the {@link BatchPoolModule} */
                Batch batch = future.get(1, TimeUnit.SECONDS);
                LOG.info("Batch {} returned with success", batch.toString());
            } catch (TimeoutException ex) {
                if (i < CONCURRENT_WRITERS) {
                    fail();
                } else {
                    LOG.info("Yaaaayyyyy! This is the blocked call!");
                }
            } finally {
                future.cancel(true); // may or may not desire this
            }

        }

    }

}