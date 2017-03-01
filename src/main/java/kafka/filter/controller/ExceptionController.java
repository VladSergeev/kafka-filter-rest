package kafka.filter.controller;


import lombok.extern.log4j.Log4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
@Log4j
public class ExceptionController {
    @ExceptionHandler(Exception.class)
    public ResponseEntity handleError(HttpServletRequest req, Exception ex) {
        log.error("Request: " + req.getRequestURL() + " raised " + ex, ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
    }
}
