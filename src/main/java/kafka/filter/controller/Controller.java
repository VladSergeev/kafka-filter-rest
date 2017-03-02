package kafka.filter.controller;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import kafka.filter.model.FilterCriteria;
import kafka.filter.service.TopicService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import javax.validation.Valid;
import java.util.List;

import static kafka.filter.Application.API;

/**
 * Created by User on 02.03.2017.
 */
@RestController
@RequestMapping(API + "/topic")
public class Controller {

    @Autowired
    private TopicService topicService;
    /**
     * @param criteria - criteria of messages
     * @return - messages
     * @throws Exception - exception
     */
    @ApiOperation(value = "Filter topic messages", notes = "Filter topic messages")
    @ApiImplicitParams(value = {
            @ApiImplicitParam(value = "Topic name", name = "topic",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(value = "Partition", name = "partition",
                    dataType = "int", paramType = "query"),
            @ApiImplicitParam(value = "Offset", name = "offset",
                    dataType = "long", paramType = "query"),
            @ApiImplicitParam(value = "Count", name = "count",
                    dataType = "int", paramType = "query"),
            @ApiImplicitParam(value = "Message key", name = "criteria.key"
                    , dataType = "string", paramType = "query"),
            @ApiImplicitParam(value = "Message value", name = "criteria.value"
                    , dataType = "string", paramType = "query")
    })
    @RequestMapping(value = "/filter", method = RequestMethod.GET)
    @ResponseBody
    public List<Tuple2<String,String>> filter(@Valid FilterCriteria criteria) throws Exception {
        //todo:make RX
        return topicService.filter(criteria);
    }
}
