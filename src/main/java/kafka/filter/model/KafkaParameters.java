package kafka.filter.model;

import java.util.Map;

/**
 * Created by vsergeev on 03.03.2017.
 */
public class KafkaParameters {
    private Map<String,Object> params;

    public KafkaParameters(Map<String, Object> params) {
        this.params = params;
    }

    public Map<String, Object> getParams() {
        return params;
    }
}
