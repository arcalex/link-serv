package models;

import java.util.Map;

public class Output {
    public Boolean doneCreation;

    public Output(Map<String, Object> data) {
        this.doneCreation = true;
    }

    public Output(boolean done) {
        this.doneCreation = done;
    }
}
