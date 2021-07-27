package org.bibalex.linkserv.validators;

import static java.util.Objects.isNull;
import static org.bibalex.linkserv.handlers.PropertiesHandler.getProperty;
import static org.bibalex.linkserv.handlers.PropertiesHandler.initializeProperties;

public class RequestParametersValidator {

    public RequestParametersValidator() {
        initializeProperties();
    }

    public boolean validateGetGraphParameters(String identifier, String timestamp, Integer depth){
        return (validateIdentifier(identifier) && validateTimestamp(timestamp) && validateDepth(depth));
    }

    public boolean validateGetVersionCountsYealyParameters(String identifier){
        return (validateIdentifier(identifier));
    }

    public boolean validateGetVersionCountsMonthlyParameters(String identifier, Integer year){
        return (validateIdentifier(identifier) && validateYear(year));
    }

    public boolean validateGetVersionCountsDailyParameters(String identifier, Integer year, Integer month){
        return (validateIdentifier(identifier) && validateYear(year) && validateMonth(month));
    }

    public boolean validateGetVersionsParameters(String identifier, String dateTime){
        return (validateIdentifier(identifier) && validateDateTime(dateTime));
    }

    public boolean validateGetLatestVersionParameters(String identifier){
        return (validateIdentifier(identifier));
    }

    private boolean validateIdentifier(String identifier) {
        return (!(isNull(identifier)) && (identifier.matches("(?:https?:\\/\\/)?(?:[^?\\/\\s]+[?\\/])(.*)")));
    }

    private boolean validateTimestamp(String timestamp) {
        if(isNull(timestamp))
            return false;
        String timeRangeDelimiter = getProperty("timeRangeDelimiter");
        String timestampRegex = "[0-9]{14}";
        if(timestamp.equals(timeRangeDelimiter)) {
            return true;
        }
        if(timestamp.contains(timeRangeDelimiter)) {
            String [] timestamps = timestamp.split(timeRangeDelimiter);
            if(timestamps.length == 1){
                return (timestamps[0].matches(timestampRegex));
            }
            else
                if(!(timestamps[1].isEmpty())){
                    return (timestamps[1].matches(timestampRegex));
                }
            return (timestamps[0].matches(timestampRegex) && timestamps[1].matches(timestampRegex));
        }
        return (timestamp.matches(timestampRegex));
    }

    private boolean validateDepth(Integer depth) {
        return (!(isNull(depth)) && (depth > 0));
    }

    private boolean validateYear(Integer year){
        return (!(isNull(year)) && (year > 0));
    }

    private boolean validateMonth(Integer month){
        return (!(isNull(month)) && (month > 0) && (month < 13));
    }

    private boolean validateDateTime(String dateTime){
        return (!(isNull(dateTime)) && dateTime.matches("[0-9]{8}"));
    }
}