package org.exaspace.kafkawait.demo;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;


public class CalculatorMessage {

    private static final ObjectMapper mapper = new ObjectMapper();

    public Long messageId;
    public String operation;
    public List<Integer> args;
    public Integer result;
    public boolean isError;

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CalculatorMessage fromJson(String input) {
        try {
            return mapper.readValue(input, CalculatorMessage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
