package models;

import constants.Constants;

import java.util.Map;

public class RootNode {
    public String nodeId;
    public String nodeURL;
    public String versionName;

    public RootNode(Map<String, Object> data) {
        this.nodeId = String.valueOf(data.get("ID(v)"));
        this.nodeURL = String.valueOf(data.get("n." + Constants.nameProperty));
        this.versionName = String.valueOf(data.get("v." + Constants.versionProperty));
    }

}