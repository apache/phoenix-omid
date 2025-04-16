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
package org.apache.omid.metrics;

import java.util.HashSet;
import java.util.Set;

public class CodahaleMetricsConfig extends AbstractMetricsConfig {

    public enum Reporter {
        CSV, SLF4J, GRAPHITE, CONSOLE
    }

    private static final String DEFAULT_PREFIX = "omid";
    private static final String DEFAULT_GRAPHITE_HOST = "localhost:2003";
    private static final String DEFAULT_CSV_DIR = ".";
    private static final String DEFAULT_SLF4J_LOGGER = "metrics";

    private static final String METRICS_CODAHALE_PREFIX_KEY = "metrics.codahale.prefix";
    private static final String METRICS_CODAHALE_REPORTERS_KEY = "metrics.codahale.reporters";
    private static final String METRICS_CODAHALE_GRAPHITE_HOST_CONFIG = "metrics.codahale.graphite.host.config";
    private static final String METRICS_CODAHALE_CSV_DIR = "metrics.codahale.cvs.dir";
    private static final String METRICS_CODAHALE_SLF4J_LOGGER = "metrics.codahale.slf4j.logger";

    private String prefix = DEFAULT_PREFIX;
    private Set<Reporter> reporters = new HashSet<Reporter>();
    private String graphiteHostConfig = DEFAULT_GRAPHITE_HOST;
    private String csvDir = DEFAULT_CSV_DIR;
    private String slf4jLogger = DEFAULT_SLF4J_LOGGER;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public Set<Reporter> getReporters() {
        return reporters;
    }

    public void setReporters(Set<Reporter> reporters) {
        this.reporters = reporters;
    }

    public void addReporter(Reporter reporter) {
        reporters.add(reporter);
    }

    public String getGraphiteHostConfig() {
        return graphiteHostConfig;
    }

    public void setGraphiteHostConfig(String graphiteHostConfig) {
        this.graphiteHostConfig = graphiteHostConfig;
    }

    public String getCsvDir() {
        return csvDir;
    }

    public void setCsvDir(String csvDir) {
        this.csvDir = csvDir;
    }

    public String getSlf4jLogger() {
        return slf4jLogger;
    }

    public void setSlf4jLogger(String slf4jLogger) {
        this.slf4jLogger = slf4jLogger;
    }

}