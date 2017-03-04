package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * Created by User on 04.03.2017.
 */
public interface KafkaProxyService {
    JavaRDD<Tuple2<String, String>> getKafkaRDD(FilterCriteria filter) throws Exception;
}
