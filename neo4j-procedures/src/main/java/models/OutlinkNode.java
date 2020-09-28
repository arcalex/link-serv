package models;

import constants.Constants;

import java.util.Map;

public class OutlinkNode {
    public String parentId;
    public String parentVersionId;
    public String outlinkVersionId;
    public String relationshipId;
    public String outlinkName;
    public String outlinkVersion;

    public OutlinkNode(Map<String, Object> data) {
        this.parentId = String.valueOf(data.get("ID(parent2)").toString());
        this.parentVersionId = String.valueOf(data.get("ID(version1)"));
        this.outlinkVersionId = String.valueOf(data.get("ID(version2)"));
        this.relationshipId = String.valueOf(data.get("ID(r)"));
        this.outlinkName = String.valueOf(data.get("parent2." + Constants.nameProperty));
        this.outlinkVersion = String.valueOf(data.get("version2." + Constants.versionProperty));
    }
}