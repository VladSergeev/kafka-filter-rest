package kafka.filter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by User on 02.03.2017.
 */
@SpringBootApplication
public class Application {
    public static final String API = "/api/v1";
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        System.out.println("http://localhost:8080");
        System.out.println("http://localhost:8080/swagger-ui.html");
    }
}
