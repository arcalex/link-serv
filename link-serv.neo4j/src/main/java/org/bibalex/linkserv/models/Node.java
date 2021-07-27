package org.bibalex.linkserv.models;

import java.util.Objects;

public class Node {
    private String id;
    private String type;
    private String url;
    private String timestamp;

    public Node(String id, String type, String url, String timestamp) {
        this.id = id;
        this.type = type;
        this.url = url;
        this.timestamp = timestamp;
    }

    public Node() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        Node node = (Node) obj;
        return this.id.equals(node.id) & this.url.equals(node.url) & this.timestamp.equals(node.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, url, timestamp);
    }
}
