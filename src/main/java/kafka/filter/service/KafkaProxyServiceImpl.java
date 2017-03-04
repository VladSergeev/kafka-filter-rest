package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import kafka.filter.model.KafkaParameters;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by vsergeev on 03.03.2017.
 */
@Service
public class KafkaProxyServiceImpl implements KafkaProxyService {
    @Value("${default.message.count:100}")
    private Long defaultCount;
    @Autowired
    private JavaSparkContext context;
    @Autowired
    private KafkaParameters kafkaParams;

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaServers;


    public JavaRDD<ConsumerRecord<String, String>> getKafkaRDD(FilterCriteria filter) throws Exception {
        OffsetRange[] arr = getOffsets(filter);
        return KafkaUtils.createRDD(context, kafkaParams.getParams(), arr, LocationStrategies.PreferConsistent());
    }

    private OffsetRange[] getOffsets(FilterCriteria filter) throws Exception {
        //TODO:THis operation is high performance, may be it is better to create consumer on service
        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            TopicPartition topicPartition = new TopicPartition(filter.getTopic(), filter.getPartition());
            consumer.assign(Collections.singletonList(topicPartition));

            consumer.seekToBeginning(Collections.singletonList(topicPartition));
            Long startOffset = consumer.position(topicPartition);

            consumer.seekToEnd(Collections.singletonList(topicPartition));
            Long endOffset = consumer.position(topicPartition);

            Long fromOffset = filter.getOffset() == null ||
                    filter.getOffset() < startOffset ?
                    startOffset : filter.getOffset();

            Long untilOffset = fromOffset + (filter.getCount() == null ||
                    fromOffset + filter.getCount() <= endOffset ?
                    defaultCount : filter.getCount());

            OffsetRange offsetRange = OffsetRange.create(filter.getTopic(),
                    filter.getPartition(),
                    fromOffset,
                    untilOffset);

            return new OffsetRange[]{offsetRange};
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_rest_filter_group_topic_checker");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }
}
