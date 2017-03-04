package kafka.filter;

import kafka.filter.model.FilterCriteria;
import kafka.filter.model.MessageCriteria;
import kafka.filter.service.KafkaProxyService;
import kafka.filter.service.TopicService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Created by User on 04.03.2017.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(locations = "classpath:test-application.properties")
public class SparkTest {

    @Autowired
    private TopicService topicService;
    @MockBean
    private KafkaProxyService kafkaProxyService;
    @Autowired
    private JavaSparkContext context;

    @Before
    public void setUp() throws Exception {
        given(this.kafkaProxyService.
                getKafkaRDD(any())
        ).willReturn(
                mockData());
    }

    @Test
    public void filterData() throws Exception {
        when(kafkaProxyService.getKafkaRDD(any(FilterCriteria.class)))
                .thenReturn(mockData());

        FilterCriteria criteria = new FilterCriteria();
        criteria.setTopic("test");
        criteria.setPartition(0);
        MessageCriteria messageCriteria = new MessageCriteria();
        messageCriteria.setKey("test");
        criteria.setCriteria(messageCriteria);
        Assert.assertTrue(topicService.filter(criteria).size() == 1);

        messageCriteria = new MessageCriteria();
        messageCriteria.setValue("2");
        criteria.setCriteria(messageCriteria);
        Assert.assertTrue(topicService.filter(criteria).size() == 1);

        messageCriteria = new MessageCriteria();
        criteria.setCriteria(messageCriteria);
        Assert.assertTrue(topicService.filter(criteria).size() == 3);
    }

    private JavaRDD<Tuple2<String, String>> mockData() {
        List<Tuple2<String, String>> list = new ArrayList<>();
        list.add(new Tuple2<>("oneKey", "1"));
        list.add(new Tuple2<>("testKey", "2"));
        list.add(new Tuple2<>("twoKey", "3"));
        return context.parallelize(list);
    }
}
