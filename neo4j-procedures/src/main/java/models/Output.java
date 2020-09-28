package models;

import constants.Constants;

import java.util.Map;

public class Output {
    public Boolean doneCreation;

    public Output(Map<String, Object> data) {
        this.doneCreation = data.containsKey("parent." + Constants.nameProperty);
    }
}
