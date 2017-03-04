package kafka.filter;

import kafka.filter.model.FilterCriteria;
import kafka.filter.model.MessageCriteria;
import kafka.filter.service.KafkaProxyService;
import kafka.filter.service.TopicService;
import kafka.filter.service.TopicServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Created by User on 04.03.2017.
 */
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
@TestPropertySource(locations = "classpath:test-application.properties")
public class SparkTest {

    @Autowired
    private TopicService topicService;
    @Mock
    private KafkaProxyService kafkaProxyService;
    @Autowired
    private JavaSparkContext context;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
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

    private JavaRDD<ConsumerRecord<String, String>> mockData() {
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        list.add(new ConsumerRecord<String, String>("test", 0, 1L, "oneKey", "1"));
        list.add(new ConsumerRecord<String, String>("test", 0, 2L, "testKey", "2"));
        list.add(new ConsumerRecord<String, String>("test", 0, 3L, "twoKey", "3"));
        return context.parallelize(list);
    }
}
