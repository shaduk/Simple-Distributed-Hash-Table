package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by shadkhan on 07/04/17.
 */

public class MessageObj implements Serializable{
    public String key;
    public String val;
    public String preNode;
    public String sucNode;
    public String fwdPort;
    public reqType reqT;
    public HashMap<String, String> store;

    public MessageObj()
    {
        val = null;
        preNode = null;
        sucNode = null;
        fwdPort = null;
        store = null;
    }
    public MessageObj(String fwdPort, String key, String preNode, String sucNode, String myNode, reqType req)
    {
        this.fwdPort = fwdPort;
        this.key = key;
        this.preNode = preNode;
        this.sucNode = sucNode;
        this.reqT = req;
    }

    enum reqType {JOIN, JOIN_REPLY, JOIN_PRV_UPDATE, INSERT, QUERY_ALL,QUERY_ONE, DELETE}
}
