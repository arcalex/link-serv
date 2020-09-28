package models;

import java.util.Map;

public class HistogramEntry {
    public Number key;
    public Number count;

    public HistogramEntry(Map<String, Object> data) {
        this.key = (Number) data.get("key");
        this.count = (Number) data.get("count");
    }

}
