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

import org.apache.phoenix.thirdparty.com.google.common.io.Resources;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class YAMLUtilsTest {

    @Test(timeOut = 10_000)
    public void testLoadDefaultSettings_setToBean() throws Exception {
        Map map = new HashMap();
        new YAMLUtils().loadSettings("test.yml", "default-test.yml", map);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.get("prop1"), 11);
        Assert.assertEquals(map.get("prop2"), "22");
        Assert.assertEquals(map.get("prop3"), 3);
    }

    @Test(timeOut = 10_000)
    public void testLoadDefaultSettings_setToBean2() throws Exception {
        Map map = new HashMap();
        new YAMLUtils().loadSettings("test.yml", map);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.get("prop1"), 11);
        Assert.assertEquals(map.get("prop2"), "22");
        Assert.assertEquals(map.size(), 2);
    }


    @Test(timeOut = 10_000)
    public void testSnakeyamlConfigUpgrade() throws Exception {
        Map map = new HashMap();
        String content = Resources.toString(Resources.getResource("config-upgrade.yml"), Charset.forName("UTF-8"));
        String expected = Resources.toString(Resources.getResource("config-upgrade-res.yml"), Charset.forName("UTF-8"));
        String upgraded = new YAMLUtils().upgradeOldConfigTOSnakeYaml2Compatible(content);
        Assert.assertNotNull(map);
        Assert.assertEquals(upgraded, expected);

        String upgraded_again = new YAMLUtils().upgradeOldConfigTOSnakeYaml2Compatible(upgraded);
        Assert.assertEquals(upgraded, upgraded_again);
    }

}