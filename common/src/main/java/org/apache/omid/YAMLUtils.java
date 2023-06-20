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
package org.apache.omid;

import org.apache.curator.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.io.Resources;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@SuppressWarnings("WeakerAccess")
public class YAMLUtils {

    private static final Logger LOG = LoggerFactory.getLogger(YAMLUtils.class);

    public Map getSettingsMap(String resourcePath, String defaultResourcePath) {
        try {
            Map properties = loadSettings(resourcePath, defaultResourcePath);
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public Map getSettingsMap(String resourcePath) {
        try {
            Map properties = loadSettings(null, resourcePath);
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void loadSettings(String resourcePath, String defaultResourcePath, Object bean) {
        try {
            Map properties = loadSettings(resourcePath, defaultResourcePath);
            BeanUtils.populate(bean, properties);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void loadSettings(String resourcePath, Object bean) {
        try {
            Map properties = loadSettings(null, resourcePath);
            BeanUtils.populate(bean, properties);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public Map loadSettings(String resourcePath, String defaultResourcePath) throws IOException {
        Map defaultSetting = loadAsMap(defaultResourcePath);
        Preconditions.checkState(defaultSetting.size() > 0, String.format("Failed to load file '%s' from classpath", defaultResourcePath));
        if (resourcePath != null) {
            Map userSetting = loadAsMap(resourcePath);
            defaultSetting.putAll(userSetting);
        }
        return defaultSetting;
    }


    public Map loadAsMap(String path) throws IOException {
        try {
            String content = Resources.toString(Resources.getResource(path), Charset.forName("UTF-8"));
            LOG.debug("content before the upgrade:");
            LOG.debug(content);
            content = upgradeOldConfigTOSnakeYaml2Compatible(content);
            LOG.debug("content after the upgrade:");
            LOG.debug(content);
            return loadStringAsMap(content);
        } catch (IllegalArgumentException e) {
            return new HashMap();
        }
    }

    public String removeTrailingSpace(String str) {
        int len = str.length();

        char[] val = str.toCharArray();
        while ((0 < len) && (val[len - 1] == ' ')) {
            len = len - 1;
        }
        return str.substring(0, len);
    }

    public int getNextIndentation(int lineNumber, String[] lines) {
        int res = 2;
        boolean notFound = true;
        while (lineNumber < lines.length && notFound) {
            String line = lines[lineNumber];
            if (line.startsWith(" ") && StringUtils.countMatches(line, " ") != line.length()){
                notFound = false;
                res = StringUtils.indexOfAnyBut(line, " ");
            } else if( !line.startsWith("#")) {
                // it is a line without indentation
                notFound = false;
            }
            lineNumber ++;
        }
        return res;
    }

    @VisibleForTesting
    protected String upgradeOldConfigTOSnakeYaml2Compatible(String old) {
        ArrayList<String> result = new ArrayList<String>();
        int p1 = 0;
        int p2 = 0;
        String[] lines = old.split("\n");

        for (int l = 0; l < lines.length; l++) {
            String line = lines[l];
            if (!line.startsWith("#")) {
                String tab = "\t";
                if (line.contains(tab)) {
                    line = line.replaceAll(tab, " ");
                }
                line = removeTrailingSpace(line);
                line = line.replace("!!org.apache.omid.tso.client.OmidClientConfiguration$ConnType ", "\"");
                line = line.replace("!!org.apache.omid.tso.client.OmidClientConfiguration$PostCommitMode ", "\"");
                line = line.replace("!!org.apache.omid.tso.client.OmidClientConfiguration$ConflictDetectionLevel ", "\"");
                line = line.replace("!!org.apache.omid.tso.client.OmidClientConfiguration", "");
                line = line.replace("!!org.apache.omid.metrics.CodahaleMetricsConfig$Reporter ", "\"");
                line = line.replace("!!org.apache.omid.metrics.CodahaleMetricsConfig {", "");
                line = line.replace(" !!org.apache.omid.metrics.CodahaleMetricsProvider [", "\n  {\n    class: \"org.apache.omid.metrics.CodahaleMetricsProvider\",");

                line = line.replace("!!set {", "[");
                line = line.replace("},", "],");
                line = line.replace(" [ ]", "");

                int nextIndentation = getNextIndentation(l + 1, lines);
                String indentation = StringUtils.repeat(" ", nextIndentation);
                line = line.replace(" !!", "\n" +indentation + "class: \"");

                line = line.replace("\"org.apache.omid.benchmarks.utils.ZipfianGenerator [ ",
                        "\"org.apache.omid.benchmarks.utils.ZipfianGenerator\"\n" + indentation + "max: ");
                line = line.replace("\"org.apache.omid.benchmarks.utils.ScrambledZipfianGenerator [ ",
                        "\"org.apache.omid.benchmarks.utils.ScrambledZipfianGenerator\"\n" + indentation + "max: ");


                ArrayList<Integer> removeChars = new ArrayList<Integer>();
                for (int i = 0; i < line.length(); i++)
                {
                    String c = line.substring(i, i+1);
                    switch (c) {
                        case "[":
                            p1 = p1 + 1;
                            break;
                        case "]":
                            if (p1 == 0) {
                                removeChars.add(i);
                            } else {
                                p1 = p1 - 1;
                            }
                            break;
                        case "{":
                            p2 = p2 + 1;
                            break;
                        case "}":
                            if (p2 == 0) {
                                removeChars.add(i);
                            } else {
                                p2 = p2 - 1;
                            }
                            break;
                    }
                }
                boolean removedChars = false;
                if (removeChars.size() > 0) {
                    StringBuilder newLine = new StringBuilder(line.substring(0, removeChars.get(0)));
                    int numberToRemove = removeChars.size() - 1;
                    for (int i = 1; i < numberToRemove; i++) {
                        newLine.append(line, removeChars.get(i) + 1, removeChars.get(i + 1));
                    }
                    newLine.append(line, removeChars.get(numberToRemove) + 1, line.length());
                    line = newLine.toString();
                    removedChars = true;
                }

                if (StringUtils.countMatches(line, "\"") % 2 == 1) {
                    if (line.endsWith(",")) {
                        result.add(line.substring(0, line.length()-1) + "\"" + ",");
                    } else {
                        result.add(line + "\"");
                    }
                } else {
                    if (StringUtils.countMatches(line, " ") < line.length() || (line.length() == 0 && !removedChars)) {
                        result.add(line);
                    }
                }
            }
        }

        return String.join("\n", result);
    }

    public Map loadStringAsMap(String content) {
        try {
            Map settings = new Yaml().loadAs(content, Map.class);
            return (settings != null) ? settings : new HashMap(0);
        } catch (IllegalArgumentException e) {
            return new HashMap();
        }
    }

}
