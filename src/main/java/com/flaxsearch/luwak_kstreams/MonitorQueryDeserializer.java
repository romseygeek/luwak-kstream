package com.flaxsearch.luwak_kstreams;
/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import uk.co.flax.luwak.MonitorQuery;

public class MonitorQueryDeserializer implements Deserializer<MonitorQuery> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public MonitorQuery deserialize(String s, byte[] bytes) {
        String q = new String(bytes, StandardCharsets.UTF_8);
        return new MonitorQuery(q, q);
    }

    @Override
    public void close() {

    }
}
