package kafka.filter.config;

import kafka.filter.model.KafkaParameters;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static kafka.filter.Application.API;
import static springfox.documentation.builders.PathSelectors.ant;

/**
 * Created by User on 02.03.2017.
 */
@EnableSwagger2
@Configuration
public class Config {

    @Value("${spark.master:local[1]}")
    private String master;

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaServers;

    @Bean
    public JavaSparkContext context() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Kafka REST filter");
        sparkConf.setMaster(master);
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public Docket newsApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("all")
                .apiInfo(apiInfo())
                .select()
                .paths(ant(API + "/**"))
                .build();
    }

    @Bean
    public KafkaParameters kafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaServers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "kafka_rest_filter_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", "false");
        return new KafkaParameters(kafkaParams);
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Kafka rest filter")
                .version("1.0")
                .build();
    }
}
