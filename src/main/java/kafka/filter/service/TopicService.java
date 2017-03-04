package kafka.filter.service;

import kafka.filter.model.FilterCriteria;
import scala.Tuple2;

import java.util.List;

/**
 * Created by User on 04.03.2017.
 */
public interface TopicService {
    List<Tuple2<String, String>> filter(FilterCriteria filter) throws Exception;
}
