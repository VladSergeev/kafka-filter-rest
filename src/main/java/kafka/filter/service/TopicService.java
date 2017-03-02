package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.*;

/**
 * Created by User on 02.03.2017.
 */
@Service
public class TopicService {

    @Autowired
    private JavaSparkContext context;

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaServers;
    @Value("${default.message.count:100}")
    private Long defaultCount;

    public List<Tuple2<String, String>> filter(FilterCriteria filter) throws Exception {
        OffsetRange[] arr = getOffsets(filter);
        JavaRDD<ConsumerRecord<String, String>> records =
                KafkaUtils.createRDD(context, kafkaParams(), arr, LocationStrategies.PreferConsistent());

        JavaRDD<Tuple2<String, String>> serialized = records.map(record -> new Tuple2<>(record.key(), record.value()));

        JavaRDD<Tuple2<String, String>> filteredRecords = null;
        if (filter.getCriteria() != null &&
                (filter.getCriteria().getKey() != null ||
                        filter.getCriteria().getValue() != null)) {

            String filterKey=filter.getCriteria().getKey();
            String filterValue=filter.getCriteria().getValue();
            filteredRecords = serialized.filter(
                    x -> x._1() != null
                            &&
                            x._1().contains(filterKey)
                            || x._2() != null &&
                            x._2().contains(filterValue)
            );
        }

        return filteredRecords != null ? filteredRecords.collect() : serialized.collect();
    }


    private Map<String, Object> kafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaServers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "kafka_rest_filter_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", "false");
        return kafkaParams;
    }

    private OffsetRange[] getOffsets(FilterCriteria filter) throws Exception {

        KafkaConsumer<String, String> consumer = null;
        //TODO:THis operation is high performance, may be it is better to create consumer on service
        try {


            consumer = createConsumer();

            TopicPartition topicPartition = new TopicPartition(filter.getTopic(), filter.getPartition());
            consumer.assign(new ArrayList<TopicPartition>() {
                {
                    add(topicPartition);
                }
            });

            consumer.seekToBeginning(new ArrayList<TopicPartition>() {
                {
                    add(topicPartition);
                }
            });
            Long startOffset = consumer.position(topicPartition);
            consumer.seekToEnd(new ArrayList<TopicPartition>() {
                {
                    add(topicPartition);
                }
            });
            Long endOffset = consumer.position(topicPartition);


            Long fromOffset = filter.getOffset() != null &&
                    filter.getOffset() < startOffset ?
                    startOffset : filter.getOffset();

            Long untilOffset = filter.getCount() != null &&
                    fromOffset + filter.getCount() <= endOffset ?
                    fromOffset + filter.getCount() : defaultCount;

            OffsetRange offsetRange = OffsetRange.create(filter.getTopic(),
                    filter.getPartition(),
                    fromOffset,
                    untilOffset);

            return new OffsetRange[]{offsetRange};
        } finally {
            if (consumer != null) {
                consumer.close();
            }
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
