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

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import uk.co.flax.luwak.MonitorQuery;

public class MonitorQuerySerializer implements Serializer<MonitorQuery> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, MonitorQuery monitorQuery) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
