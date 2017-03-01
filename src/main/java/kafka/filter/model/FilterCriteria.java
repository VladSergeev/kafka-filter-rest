package kafka.filter.model;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by User on 02.03.2017.
 */
@Getter
@Setter
public class FilterCriteria {
    private String topic;
    private Integer partition;
    private Long offset;
    private Integer count;
    private MessageCriteria criteria;

    @Override
    public String toString() {
        return "FilterCriteria{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", count=" + count +
                ", criteria=" + criteria +
                '}';
    }
}
