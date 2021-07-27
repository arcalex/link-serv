package org.bibalex.linkserv.controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bibalex.linkserv.validators.RequestParametersValidator;
import org.bibalex.linkserv.services.LinkServService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.bibalex.linkserv.handlers.PropertiesHandler.getProperty;
import static org.bibalex.linkserv.handlers.PropertiesHandler.initializeProperties;

@RestController
@RequestMapping("/")
public class LinkServController {

    @Autowired
    private LinkServService linkServService;

    private static final Logger LOGGER = LogManager.getLogger(LinkServController.class);

    @RequestMapping(method = RequestMethod.POST, value = "/**")
    public ResponseEntity<Object> updateGraph(HttpServletRequest request,
                                              @RequestBody String jsonGraph,
                                              @RequestParam String operation) {

        initializeProperties();
        String requestURL = request.getRequestURL().toString();
        String[] urlParams = requestURL.split("/", 4);
        String workspaceName = ((urlParams.length == 1) ? "*" : urlParams[3]);
        LOGGER.info("Updating Graph with Parameters: " + workspaceName);
        if (operation.equals(getProperty("updateGraph"))) {
            String response = linkServService.updateGraph(jsonGraph, workspaceName);
            if (response.equals(getProperty("badRequestResponseStatus"))) {
                LOGGER.error("Response Status: 400 - Bad Request");
                return ResponseEntity.badRequest().body("Please, send only one VersionNode with timestamp and identifier" +
                        " typical to those present in the request body.");
            }
            if(response.isEmpty()){
                LOGGER.info("Response Status: 500 - Internal Server Error");
                return (ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Could not update graph"));
            }
            LOGGER.info("Response Status: 200 - OK");
            return ResponseEntity.ok(response);
        } else {
            LOGGER.error("Response Status: 500 - Operation Not Found: " + operation);
            throw new RuntimeException("Operation Not Found: " + operation);
        }
    }

    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET, value = "/**")
    public ResponseEntity<String> getOperations(HttpServletRequest request,
                                                @RequestParam String operation,
                                                @RequestParam String identifier,
                                                @RequestParam(required = false) String timestamp,
                                                @RequestParam(required = false, defaultValue = "1") Integer depth,
                                                @RequestParam(required = false) Integer year,
                                                @RequestParam(required = false) Integer month,
                                                @RequestParam(required = false) String dateTime) {

        initializeProperties();
        RequestParametersValidator requestParametersValidator = new RequestParametersValidator();
        try {
            identifier = URLDecoder.decode(identifier, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        LOGGER.info("Request received: " + operation);
        switch (operation) {

            case "getGraph":
                if(requestParametersValidator.validateGetGraphParameters(identifier, timestamp, depth)){
                    String response = linkServService.getGraph(identifier, timestamp, depth);
                    LOGGER.info("Response Status: 200 - OK");
                    return ResponseEntity.ok(response);
                }
                LOGGER.error("Response Status: 400 - Bad Request");
                LOGGER.error("Invalid request parameters");
                return ResponseEntity.badRequest().body("Please, send a valid identifier, depth, and timestamp(s)");

            case "getVersionCountsYearly":
                if (requestParametersValidator.validateGetVersionCountsYealyParameters(identifier)){
                    return ResponseEntity.ok(linkServService.getVersionCountsYearly(identifier));
                }
                LOGGER.error("Response Status: 400 - Bad Request");
                LOGGER.error("Invalid identifier");
                return ResponseEntity.badRequest().body("Please, send a valid identifier");

            case "getVersionCountsMonthly":
                if (requestParametersValidator.validateGetVersionCountsMonthlyParameters(identifier, year)){
                    return ResponseEntity.ok(linkServService.getVersionCountsMonthly(identifier, year));
                }
                LOGGER.error("Response Status: 400 - Bad Request");
                LOGGER.error("Invalid identifier or year");
                return ResponseEntity.badRequest().body("Please, send a valid identifier and year");

            case "getVersionCountsDaily":
                if (requestParametersValidator.validateGetVersionCountsDailyParameters(identifier, year, month)) {
                    return ResponseEntity.ok(linkServService.getVersionsCountDaily(identifier, year, month));
                }
                LOGGER.error("Response Status: 400 - Bad Request");
                LOGGER.error("Invalid identifier, year, or month");
                return ResponseEntity.badRequest().body("Please, send a valid identifier, year, and month");

            case "getVersions":
                if (requestParametersValidator.validateGetVersionsParameters(identifier, dateTime)) {
                    return ResponseEntity.ok(linkServService.getVersions(identifier, dateTime));
                }
                LOGGER.error("Response Status: 400 - Bad Request");
                LOGGER.error("Invalid identifier or date-time");
                return ResponseEntity.badRequest().body("Please, send a valid identifier and date-time");

            case "getLatestVersion":
                if (requestParametersValidator.validateGetLatestVersionParameters(identifier)) {
                    return ResponseEntity.ok(linkServService.getLatestVersion(identifier));
                }
                LOGGER.error("Response Status: 400 - Bad Request");
                LOGGER.error("Invalid identifier");
                return ResponseEntity.badRequest().body("Please, send a valid identifier");

            default:
                LOGGER.error("Response Status: 500, Operation Not Found: " + operation);
                throw new RuntimeException("Operation Not Found: " + operation);
        }
    }
}