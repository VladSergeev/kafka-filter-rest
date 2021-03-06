package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.*;

/**
 * Created by User on 02.03.2017.
 */
@Service
public class TopicServiceImpl implements TopicService {


    @Autowired
    private KafkaProxyService kafkaProxyService;


    public List<Tuple2<String, String>> filter(FilterCriteria filter) throws Exception {

        JavaRDD<Tuple2<String, String>> records = kafkaProxyService.getKafkaRDD(filter);
        if (filter.getCriteria() != null &&
                (filter.getCriteria().getKey() != null ||
                        filter.getCriteria().getValue() != null)) {

            String filterKey = filter.getCriteria().getKey();
            String filterValue = filter.getCriteria().getValue();
            records = records.filter(
                    x -> x._1() != null && filterKey != null && x._1().contains(filterKey)
                            || x._2() != null && filterValue != null && x._2().contains(filterValue)
            );
        }
        return records.collect();
    }


}
