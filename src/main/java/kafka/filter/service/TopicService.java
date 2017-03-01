package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by User on 02.03.2017.
 */
@Service
public class TopicService {

    @Autowired
    private JavaSparkContext context;

    @Value("{kafka.bootstrap.servers:localhost:9092}")
    private String kafkaServers;

    public List<ConsumerRecord<String, String>> filter(FilterCriteria filter) {

        //TODO:create a check of overhead offset
        OffsetRange offsetRange = OffsetRange.create(filter.getTopic(),
                filter.getPartition(),
                filter.getOffset(),
                filter.getOffset() + filter.getCount());
        OffsetRange[] arr = {offsetRange};
        JavaRDD<ConsumerRecord<String, String>> records =
                KafkaUtils.createRDD(context, kafkaParams(), arr, LocationStrategies.PreferConsistent());
        JavaRDD<ConsumerRecord<String, String>> filteredRecords = null;
        if (filter.getCriteria() != null &&
                (filter.getCriteria().getKey() != null ||
                        filter.getCriteria().getValue() != null)) {


            filteredRecords = records.filter(
                    x -> x.key() != null
                            &&
                            x.key().contains(filter.getCriteria().getKey())
                            || x.value() != null &&
                            x.value().contains(filter.getCriteria().getValue())
            );
        }

        return filteredRecords != null ? filteredRecords.collect() : records.collect();
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
}
