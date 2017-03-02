package kafka.filter.model;

import lombok.Getter;
import lombok.Setter;


import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * Created by User on 02.03.2017.
 */
@Getter
@Setter
public class FilterCriteria implements Serializable {
    @NotNull(message = "Topic is not chosen!")
    private String topic;
    @NotNull(message = "Partition is not chosen!")
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
