package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.provider.Telephony;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    private DatabaseHelper mDb;
    private static final String DATABASE_NAME = "dictionary";
    SQLiteDatabase mydatabase;
    static final int SERVER_PORT = 10000;
    HashMap<String, String> globalStore = new HashMap<String, String>();
    private String nextNode;
    private String prevNode;
    private boolean wait = true;
    private String thisNode;
    private final String superNode = "11108";

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        thisNode = myPort;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            //Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        nextNode = thisNode;
        prevNode = thisNode;

        if(!thisNode.equals(superNode)) {
            MessageObj mObj = new MessageObj();
            mObj.fwdPort = superNode;
            mObj.key = thisNode;
            mObj.reqT = MessageObj.reqType.JOIN;
            newClient(mObj);
        }

        boolean b;
        if (getContext().deleteDatabase(DATABASE_NAME)) b = true; //code taken from stack overflow
        else b = false;
        mDb = new DatabaseHelper(getContext());
        //Log.v(TAG,"Created database");
        return true;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("@")) {
            mydatabase.rawQuery("delete * from "+DATABASE_NAME, null);
            //Log.v(TAG,"Deleted database");
        }
        else if(selection.equals("*")) {
            mydatabase.rawQuery("delete * from "+DATABASE_NAME, null);
            MessageObj mDelete = new MessageObj();
            mDelete.reqT = MessageObj.reqType.DELETE;
            mDelete.fwdPort = nextNode;
            mDelete.key = "*";
            mDelete.preNode = thisNode;
            newClient(mDelete);
        } else {
            if(belongsHere(selection)) {
                String whereClause = "key=?";
                String[] whereArgs = new String[] {selection};
                mydatabase.delete(DATABASE_NAME, whereClause, whereArgs);
            } else {
                MessageObj mDelete = new MessageObj();
                mDelete.reqT = MessageObj.reqType.DELETE;
                mDelete.fwdPort = nextNode;
                mDelete.key = selection;
                newClient(mDelete);
            }
        }
        return 0;
    }

    public void newClient(MessageObj messageObj)
    {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageObj);
        return;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        /*
        if(prevNode != null)
            Log.v("prevNode", prevNode);
        if(nextNode != null)
            Log.v("nextNode", nextNode); */

        mydatabase = mDb.getWritableDatabase();
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        //Log.v("Does is belong here?", String.valueOf(belongsHere(key)));
        if(belongsHere(key)) {
            /*
            String log = "Message : " + key + " belongs at avd - " + thisNode;
            Log.v("Filter ", log);
            */

            long id = mydatabase.insert(DATABASE_NAME, null, values);
            if (id == -1){
                //Log.e(TAG,"Failed to insert key");
                return null;
            }
            //Log.v("inserted", values.toString());
        }
        else {

            MessageObj mInsert = new MessageObj();
            mInsert.key = key;
            mInsert.val = value;
            mInsert.fwdPort = nextNode;
            mInsert.reqT = MessageObj.reqType.INSERT;
            newClient(mInsert);
        }
        return uri;
    }

    public boolean belongsHere(String key)
    {
        String prevNodeId = null;
        String keyHash = null;
        String thisNodeId = null;
        try {
            keyHash = genHash(key);
            thisNodeId = genHash(String.valueOf((Integer.parseInt(thisNode)/2)));
            //Log.v("hash of node", thisNodeId + "-" + thisNode + "-"+String.valueOf((Integer.parseInt(thisNode)/2)));
            prevNodeId = genHash(String.valueOf((Integer.parseInt(prevNode)/2)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(thisNodeId.compareTo(prevNodeId) == 0)
            return true;
        else if(keyHash.compareTo(prevNodeId) > 0 && keyHash.compareTo(thisNodeId) <= 0 && prevNodeId.compareTo(thisNodeId) < 0)
            return true;
        else if(keyHash.compareTo(thisNodeId) >= 0 && keyHash.compareTo(prevNodeId) > 0 && prevNodeId.compareTo(thisNodeId) > 0)
            return true;
        else if(keyHash.compareTo(thisNodeId) <= 0 && keyHash.compareTo(prevNodeId) < 0 && thisNodeId.compareTo(prevNodeId) < 0)
            return true;
        else
            return false;

    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        SQLiteDatabase mydatabase = mDb.getReadableDatabase();
        Cursor cursor = null;

        if(selection.equals("*")) {

            if(thisNode.equals(prevNode)) {
                cursor = mydatabase.rawQuery("select * from " + DATABASE_NAME, null);
            }
            else {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                MessageObj mQuery = new MessageObj();
                mQuery.fwdPort = nextNode;
                mQuery.store = new HashMap<String, String>();
                mQuery.key = thisNode;
                mQuery.reqT = MessageObj.reqType.QUERY_ALL;
                newClient(mQuery);
                while(wait) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                wait = true;
                Cursor temp = mydatabase.rawQuery("select * from " + DATABASE_NAME, null);
                while (temp.moveToNext()) {
                    globalStore.put(temp.getString(temp.getColumnIndex("key")), temp.getString(temp.getColumnIndex("value")));
                }
                for (Map.Entry<String, String> entry : globalStore.entrySet()) {
                    matrixCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
                globalStore = null;
                return matrixCursor;
            }

        } else if(selection.equals("@")) {
            cursor = mydatabase.rawQuery("select * from "+DATABASE_NAME,null);
            //Log.v("query @", selection);
        }
        else {
            if(belongsHere(selection)) {
                String [] sArgs ={selection};
                cursor = mydatabase.query(DATABASE_NAME,null,"key = ?",sArgs,null,null,null);
                //Log.v("query *", selection);
            } else {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                MessageObj mQuery = new MessageObj();
                mQuery.fwdPort = nextNode;
                mQuery.store = new HashMap<String, String>();
                mQuery.key = selection;
                mQuery.reqT = MessageObj.reqType.QUERY_ONE;
                mQuery.preNode = thisNode;
                newClient(mQuery);
                while(wait) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                wait = true;
                for (Map.Entry<String, String> entry : globalStore.entrySet()) {
                    matrixCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
                globalStore.clear();
                return matrixCursor;
            }

        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, MessageObj, Void> {
        protected Void doInBackground (ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream objectInputStream =
                            new ObjectInputStream(socket.getInputStream());
                    MessageObj message = (MessageObj) objectInputStream.readObject();

                    if(message.reqT.equals(MessageObj.reqType.JOIN)) {
                        Log.v("Join request by ", message.key);
                        String thisMessageKey = String.valueOf((Integer.parseInt(message.key)/2));

                        if(belongsHere(thisMessageKey)){
                            MessageObj mPrevUpdate = new MessageObj();
                            mPrevUpdate.fwdPort = prevNode;
                            mPrevUpdate.reqT = MessageObj.reqType.JOIN_PRV_UPDATE;
                            mPrevUpdate.sucNode = message.key;
                            newClient(mPrevUpdate);
                            MessageObj mJoinReply = new MessageObj();
                            mJoinReply.fwdPort = message.key;
                            mJoinReply.sucNode = thisNode;
                            mJoinReply.preNode = prevNode;
                            mJoinReply.reqT = MessageObj.reqType.JOIN_REPLY;
                            newClient(mJoinReply);
                            prevNode = message.key;
                        }
                        else {
                            message.fwdPort = nextNode;
                            newClient(message);
                        }
                    }
                    else if (message.reqT.equals(MessageObj.reqType.JOIN_PRV_UPDATE)) {
                        nextNode = message.sucNode;
                    }
                    else if(message.reqT.equals(MessageObj.reqType.JOIN_REPLY)) {
                        nextNode = message.sucNode;
                        prevNode = message.preNode;
                    } else if (message.reqT.equals(MessageObj.reqType.INSERT)) {
                        /* String log2 = "Avd : " + thisNode + " received message " + message.key;
                        Log.v("Log2", log2); */

                        if(belongsHere(message.key)) {
                            ContentValues cv = new ContentValues();
                            cv.put("key", message.key);
                            cv.put("value", message.val);
                            insert(null, cv);
                        }
                        else {
                            //Log.v("Forwarding req to ", nextNode);
                            message.fwdPort = nextNode;
                            newClient(message);
                        }
                    } else if(message.reqT.equals(MessageObj.reqType.QUERY_ALL)) {
                        if(!message.key.equals(thisNode)) {
                            Cursor temp = mydatabase.rawQuery("select * from " + DATABASE_NAME, null);
                            while (temp.moveToNext()) {
                                message.store.put(temp.getString(temp.getColumnIndex("key")), temp.getString(temp.getColumnIndex("value")));
                            }
                            message.fwdPort = nextNode;
                            newClient(message);
                        } else {
                            globalStore.clear();
                            globalStore.putAll(message.store);
                            wait = false;
                        }

                    } else if(message.reqT.equals(MessageObj.reqType.QUERY_ONE)) {
                        String m = "Avd - " + thisNode + " has received " + message.key;
                        //Log.v("qry", m);
                        if(belongsHere(message.key)) {
                            String [] sArgs ={message.key};
                            Cursor temp = mydatabase.query(DATABASE_NAME,null,"key = ?",sArgs,null,null,null);
                            while(temp.moveToNext()) {
                                message.store.put(temp.getString(temp.getColumnIndex("key")), temp.getString(temp.getColumnIndex("value")));
                            }
                            //Log.v("Avd has put ", message.key);
                            message.fwdPort = nextNode;
                            newClient(message);
                        } else if (message.preNode.equals(thisNode)) {
                            globalStore.clear();
                            globalStore.putAll(message.store);
                            wait = false;
                            //Log.v("wait changed to", String.valueOf(wait));
                        }
                        else {
                            message.fwdPort = nextNode;
                            newClient(message);
                        }
                    }
                    else if(message.reqT.equals(MessageObj.reqType.DELETE)) {
                        if(message.key.equals("*")) {
                            if(!message.preNode.equals(thisNode)) {
                                mydatabase.rawQuery("delete * from "+DATABASE_NAME, null);
                                message.fwdPort = nextNode;
                            }
                        } else {
                            if(belongsHere(message.key)) {
                                String whereClause = "key=?";
                                String[] whereArgs = new String[] {message.key};
                                mydatabase.delete(DATABASE_NAME, whereClause, whereArgs);
                            } else {
                                message.fwdPort = nextNode;
                                newClient(message);
                            }
                        }
                    }
                }
            }
            catch (IOException e)
            {
                Log.e(TAG, "Server IO Exception");
            }
            catch (ClassNotFoundException e)
            {
                Log.e(TAG, "Server Class Exception");
            }
            return null;
        }
    }


    private class ClientTask extends AsyncTask<MessageObj, Void, Void> {
        @Override
        protected Void doInBackground(MessageObj... messages) {
            try {
                MessageObj mess = messages[0];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(mess.fwdPort));
                //String log3 = "Avd " + thisNode + " forwards : " + mess.key + " to avd - " + mess.fwdPort + " with type " + mess.reqT;
                //Log.v("log3", log3);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                        socket.getOutputStream());
                objectOutputStream.writeObject(mess);
                objectOutputStream.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException" + e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException" + e.getMessage());
            }
            return null;
        }
    }
}
