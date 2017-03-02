package kafka.filter.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by User on 02.03.2017.
 */
@Getter
@Setter
public class MessageCriteria implements Serializable {
    private String key;
    private String value;

    @Override
    public String toString() {
        return "MessageCriteria{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
