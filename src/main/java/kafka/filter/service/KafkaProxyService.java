package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by User on 04.03.2017.
 */
public interface KafkaProxyService {
    JavaRDD<ConsumerRecord<String, String>> getKafkaRDD(FilterCriteria filter) throws Exception;
}
