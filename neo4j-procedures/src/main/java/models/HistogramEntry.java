package models;

import java.util.Map;

public class HistogramEntry {
    public Number key;
    public Number count;

    public HistogramEntry(Map<String, Object> data) {
        this.key = Long.valueOf((String) data.get("key"));
        this.count = Long.valueOf((Long) data.get("count"));
    }

}
