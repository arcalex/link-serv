package org.bibalex.linkserv.models;

public class Node {
    private String id;
    private String type;
    private String identifier;
    private String timestamp;

    public Node(){}

    public Node(String id, String type, String identifier, String timestamp) {
        this.id = id;
        this.type = type;
        this.identifier = identifier;
        this.timestamp = timestamp;
    }

    public Node(String type, String identifier, String timestamp) {
        this.type = type;
        this.identifier = identifier;
        this.timestamp = timestamp;
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

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
