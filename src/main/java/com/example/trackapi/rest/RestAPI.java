package com.example.trackapi.rest;

import com.example.trackapi.streams.MessageProducer;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

/**
 * Created by mlalapet on 11/02/16.
 */
@RestController
@RequestMapping("/send/rest")
public class RestAPI {

    private final static Logger logger = Logger.getLogger(RestAPI.class);

    private MessageProducer producer;

    @Autowired
    public RestAPI(MessageProducer producer){
        this.producer = producer;
    }

    @RequestMapping(
            value = "/create-sync/{id}",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Result createSync(@PathVariable("id") String id,
                       @RequestBody String payload)
            throws Exception {

        String messageKey = id;
        return sendMessage(messageKey, payload, MessageProducer.OPERATION.SYNC);
    }

    @RequestMapping(
            value = "/create-async/{id}",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Result createAsync(@PathVariable("id") String id,
                         @RequestBody String payload)
            throws Exception {

        String messageKey = id;
        return sendMessage(messageKey, payload, MessageProducer.OPERATION.ASYNC);
    }


    private Result sendMessage(String id, String payload, MessageProducer.OPERATION operation) throws JSONException {
        if(logger.isDebugEnabled()){
            logger.debug(operation+" Key "+id+" Payload "+payload);
        }

        boolean success = producer.sendMessage(id, payload, operation);
        //boolean success = producer.sendMessageCopy();
        if(success)
            return new Result(id, Result.Status.SUCCESS);
        else
            return new Result(null, Result.Status.FAIL);
    }


}
