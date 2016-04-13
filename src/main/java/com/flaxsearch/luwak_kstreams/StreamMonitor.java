package com.flaxsearch.luwak_kstreams;/*
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

import java.io.IOException;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

public class StreamMonitor {

    private static final Logger logger = LoggerFactory.getLogger(StreamMonitor.class);

    public static final String TEXT = "text";

    public static final Analyzer ANALYZER = new StandardAnalyzer();

    public static final String QUERIES_TOPIC = "queries";
    public static final String MESSAGES_TOPIC = "messages";
    public static final String MATCHES_TOPIC = "matches";

    public static void main(String... args) throws IOException {

        StreamsConfig config = Configuration.getConfig();
        KStreamBuilder builder = new KStreamBuilder();

        logger.info("Started streams listening to {}, {}, writing to {}", QUERIES_TOPIC, MESSAGES_TOPIC, MATCHES_TOPIC);

        Monitor monitor = new Monitor(new LuceneQueryParser(TEXT), new TermFilteredPresearcher());

        // Query inputs
        KTable<String, MonitorQuery> queries
                = builder.table(new StringSerializer(), new MonitorQuerySerializer(),
                                new StringDeserializer(), new MonitorQueryDeserializer(), QUERIES_TOPIC);
        KTable<String, MonitorQuery> querystatus = queries.mapValues((query) -> {
            try {
                monitor.update(query);
                logger.info("Added new query {}: {}", query.getId(), query.getQuery());
                return null;
            }
            catch (Exception e) {
                logger.error("Error adding query {}: {}", query.getId(), e.toString());
                return null;
            }
        });

        // Monitor messages
        KStream<String, String> messages = builder.stream(MESSAGES_TOPIC);
        KStream<String, Matches<QueryMatch>> matches = messages.map((id, message) -> {
            InputDocument doc = InputDocument.builder(id).addField(TEXT, message, ANALYZER).build();
            try {
                Matches<QueryMatch> m = monitor.match(doc, SimpleMatcher.FACTORY);
                logger.info("Matched document {}, with {} matches", id, m.getMatchCount(id));
                return KeyValue.pair(id, m);
            }
            catch (IOException e) {
                // TODO: separate error stream?
                return KeyValue.pair(id, null);
            }
        });
        matches.to(MATCHES_TOPIC, new StringSerializer(), new MatchesSerializer());

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

    }

}
