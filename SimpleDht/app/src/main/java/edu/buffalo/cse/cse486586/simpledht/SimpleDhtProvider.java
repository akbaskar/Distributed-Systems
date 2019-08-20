package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static String predecessor = null;
    private static String successor = null;
    private static String predecessorHash = null;
    private static String successorHash = null;

    static final String REMOTE_PORT0 = "11108";
    //    static final String REMOTE_PORT1 = "11112";
//    static final String REMOTE_PORT2 = "11116";
//    static final String REMOTE_PORT3 = "11120";
//    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;

    static final String PORT0 = "5554";
    static final String PORT1 = "5556";
    static final String PORT2 = "5558";
    static final String PORT3 = "5560";
    static final String PORT4 = "5562";

//    static final String[] remotePort = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};


    static ArrayList<String> arrayList = new ArrayList<String>();
    static HashMap<String, String> map = new HashMap<String, String>();
    static String myPort;
    static String myPortHash;
    static ArrayList<String> fileList = new ArrayList<String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if (selection.equals("@")){
            Log.i(TAG,"Please delete @ ");
            int count = fileList.size();
            for(int i=0; i<fileList.size() ; i++){
                String filename = fileList.get(i);
                if(getContext().deleteFile(filename))
                    Log.i(TAG,"File deleted successfully");
                else
                    Log.i(TAG,"Failed to delete the file");
            }
            fileList.clear();
            Log.i(TAG,"fileList cleared.");
            return count;
        }
        else if (selection.equals("*")){
            Log.i(TAG,"Please delete * ");
            int count = 0;
            if (isOnlyOnePresent()){
                count = fileList.size();
                for(int i=0; i<fileList.size() ; i++){
                    String filename = fileList.get(i);
                    if(getContext().deleteFile(filename))
                        Log.i(TAG,"File deleted successfully");
                    else
                        Log.i(TAG,"Failed to delete the file");
                }
                fileList.clear();
                Log.i(TAG,"fileList cleared.");
                return count;
            }
            else {
                String portListStr = getLivePortList();
                Log.i(TAG, "In delete() *  Port list String: " + portListStr);
                String[] portList = portListStr.split(",");
                Log.i(TAG, "In delete() *  Port list array: " + portList.toString());

                for (int i = 0; i < portList.length; i++) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portList[i]) * 2);

                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("deleteAll");
                        out.flush();

                        DataInputStream in = new DataInputStream((socket.getInputStream()));
                        String ack = in.readUTF();


                        if (ack != null) {
                            Log.i(TAG, "Deleted  " + ack + " files from " + portList[i]);
                            count = count + Integer.parseInt(ack);
                            in.close();
                            out.close();
                            socket.close();
                        }

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException " + e.getMessage() + e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException:" + e.getMessage());
                    }
                }

                Log.i(TAG, "Total deleted count: " + count);
                return count;
            }
        }

        else {   // If we need to delete a particular key
            String filename = selection;
            String keyHash = null;
            try {
                keyHash = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            boolean isHere = isThisBelongsToMe(keyHash);
            if (isHere){
                Log.i(TAG, "" + filename + " is in this AVD");
                if(getContext().deleteFile(filename))
                    if(fileList.remove(filename))
                        Log.i(TAG,"File deleted successfully");
                else
                    Log.i(TAG,"Failed to delete the file");
                return 1;
            } else {  // File is not here and we have to delete it from the successor

                try{
                    Log.i(TAG, "The file is not in this AVD. Search in the successor");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor) *2);

                    String msgToSend = "deleteWithKey";
                    Log.i(TAG, "delete method sending the key to: " + successor);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(msgToSend);
                    out.writeUTF(myPort);
                    out.writeUTF(selection);
                    out.flush();

                    DataInputStream in = new DataInputStream((socket.getInputStream()));
                    String ack = in.readUTF();

                    if (ack != null ) {
                        int count = Integer.parseInt(ack);
                        Log.i(TAG, "  " + count + " files deleted from the successor: " + successor );
                        Log.i(TAG, "inside if - closing the socket at delete() that connected to its successor");
                        in.close();
                        out.close();
                        socket.close();
                        return count;
                    }
                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
                }
            }


        }

//        String filename = selection;
//        System.out.println("Delete this file: " + filename);
//
////        File file = new File(filename);
////        if(file.delete())
//        if(getContext().deleteFile(filename))
//        {
//            System.out.println("File deleted successfully");
//        }
//        else
//        {
//            System.out.println("Failed to delete the file");
//        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = (String) values.get(KEY_FIELD);
        String val = (String) values.get(VALUE_FIELD);
        Log.i(TAG,"Please insert this key: " + key);
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.i(TAG, "Hashed key: " + keyHash);

        boolean insertHere = isThisBelongsToMe(keyHash);
        Log.i(TAG, "insert here " + insertHere);
        if(insertHere) {
            // Below code is taken from PA1
            Log.i(TAG, "Inserting in this AVD. Key: "+ key);
            String filename = key;
            String string = val + "\n";
            FileOutputStream outputStream;

            try {
                Log.i(TAG, "inside try block");
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
                Log.i(TAG,"Value inserted successfully: " + values.toString());
                fileList.add(filename);
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
            Log.i(TAG, "fileList: " + Arrays.toString(fileList.toArray()));
            return uri;
        } else {  // Tell the successor node to insert
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(successor) *2);

                String msgToSend = "insert";
                String keyValuePairToInsert =  key + "," + val;

                Log.i(TAG, "Insert method sending to: " + successor);
                Log.i(TAG, "Key Value pair as a single String: " + keyValuePairToInsert);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(msgToSend);
                out.writeUTF(key);
                out.writeUTF(val);
                out.flush();

                DataInputStream in = new DataInputStream((socket.getInputStream()));
                String ack = in.readUTF();

                if (ack.equals("ack") ) {
                    Log.i(TAG, "received 'ack', inside if - closing the socket at insert()" );
                    in.close();
                    out.close();
                    socket.close();
                }

            }catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            map.put(genHash(PORT0),PORT0);
            map.put(genHash(PORT1),PORT1);
            map.put(genHash(PORT2),PORT2);
            map.put(genHash(PORT3),PORT3);
            map.put(genHash(PORT4),PORT4);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPort = portStr;
        Log.i(TAG, "portStr: " + portStr);

        try {
            myPortHash = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        if (portStr.equals("5554")){
            Log.i(TAG,"it is port 5554");
            try {
                arrayList.add(genHash(portStr));
                Log.i(TAG, "5554 added to the array");
                Log.i(TAG, "" + Arrays.toString(arrayList.toArray()));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Collections.sort(arrayList);
        }else {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, null);
        }
        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket clientSocket = null;
            while(true) {
                try {
                    clientSocket = serverSocket.accept();
                    Log.i(TAG, "serverSocket accept ");
                    DataInputStream in = new DataInputStream((clientSocket.getInputStream()));
                    String msgType = in.readUTF();

                    if (msgType.equals("portJoin")) {
                        String newPortNo = in.readUTF();
                        Log.i(TAG, "Port number recieved at 5554 server: " + newPortNo);

                        arrayList.add(genHash(newPortNo));
                        Collections.sort(arrayList);
                        Log.i(TAG, "" + newPortNo + " added to arrayList ");
                        Log.i(TAG, "" + Arrays.toString(arrayList.toArray()));

                        String[] preSucPair = computePredecessorSuccessor(newPortNo);
                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
//                        Log.i(TAG, "server side ack sent: " + "ack");
                        out.writeUTF("predecessorSuccessorValues");
                        if (preSucPair != null) {
                            out.writeUTF(preSucPair[0]);
                            out.writeUTF(preSucPair[1]);
                        } else {
                            Log.i(TAG, "Something wrong with the computePredecessorSuccessor() function. it is returning null values");
                            out.writeUTF(null);
                            out.writeUTF(null);
                        }
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("updateYourPredecessor")){
                        String newPredecessorHash = in.readUTF();
                        Log.i(TAG, "Changing the predecessor of " + myPort + " to " + newPredecessorHash);
                        predecessorHash = newPredecessorHash;
                        predecessor = map.get(predecessorHash);
                        Log.i(TAG, "Changing the predecessor of " + myPort + " to " + predecessor);

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        Log.i(TAG, "server side ack sent: " + "ack");
                        out.writeUTF("ack");
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("updateYourSuccessor")){
                        String newSuccessorHash = in.readUTF();
                        Log.i(TAG, "Changing the successor of " + myPort + " to " + newSuccessorHash);
                        successorHash = newSuccessorHash;
                        successor = map.get(successorHash);
                        Log.i(TAG, "Changing the successor of " + myPort + " to " + successor);

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        Log.i(TAG, "server side ack sent: " + "ack");
                        out.writeUTF("ack");
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("insert")){
                        String key = in.readUTF();
                        String val = in.readUTF();
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, key);
                        cv.put(VALUE_FIELD, val);
                        Log.i(TAG, "Received insert request from predecessor, key: " + key + ". Calling insert() ");
                        insert(null,cv);

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        Log.i(TAG, "server side ack sent: " + "ack");
                        out.writeUTF("ack");
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("givePortList")){
                        ArrayList<String> portList = new ArrayList<String>();
                        String portListStr="";
                        for (int i = 0; i<arrayList.size(); i++){
                            String unHashed = map.get(arrayList.get(i));
                            portList.add(unHashed);
                            portListStr = portListStr + unHashed + ",";
                        }
                        Log.i(TAG, "arrayList: " + Arrays.toString(arrayList.toArray()));
                        Log.i(TAG, "UnHashed Port List: " + Arrays.toString(portList.toArray()));
                        portListStr = portListStr.substring(0, portListStr.length() - 1);
                        Log.i(TAG,"Port list String to the caller: " + portListStr);

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        out.writeUTF(portListStr);
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("deleteAll")){
                        int count = fileList.size();
                        for(int i=0; i<fileList.size() ; i++){
                            String filename = fileList.get(i);
                            if(getContext().deleteFile(filename))
                                Log.i(TAG,"File deleted successfully");
                            else
                                Log.i(TAG,"Failed to delete the file");
                        }
                        fileList.clear();
                        Log.i(TAG,"fileList cleared.");

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        out.writeUTF(String.valueOf(count));
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("queryAll")){
                        Log.i(TAG, "*********Query all********");
                        String output = "";
                        if (fileList.size() == 0){
                            output = "$,$&";
                        }
                        for(int i = 0; i < fileList.size(); i++){
                            String filename = fileList.get(i);
                            Log.i(TAG, "Quering " + filename);
                            String value = null;
                            FileInputStream inputStream;
                            try {
                                inputStream = getContext().openFileInput(filename);
                                int size = inputStream.available();
                                byte[] buffer = new byte[size-1];
                                inputStream.read(buffer);
                                inputStream.close();
                                value = new String(buffer);
                                Log.i(TAG, "Query value: " + value);
                            } catch (Exception e) {
                                Log.e(TAG, "File read failed" + e.getMessage());
                            }
                            output = output + filename + "," + value + "&";
                        }
                        Log.i(TAG, "All key values in this port: " +output);

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        Log.i(TAG, "server side sending all the KV pairs");
                        out.writeUTF(output);
                        out.flush();
                        out.close();
                        in.close();
                    }

                    else if (msgType.equals("deleteWithKey")) {
                        String portNo = in.readUTF();
                        String selection = in.readUTF();
                        Log.i(TAG, "At a successors server to delete with key initiated at: " + portNo);
                        if (portNo.equals(myPort)) {
                            Log.i(TAG, "Key is not found in any of the AVDs");
                        } else {
                            String keyHash = genHash(selection);
                            boolean isHere = isThisBelongsToMe(keyHash);
                            if (isHere) {
                                String filename = selection;
                                Log.i(TAG, "" + selection + " is here in this AVD");
                                if(getContext().deleteFile(filename))
                                    if(fileList.remove(filename))
                                        Log.i(TAG,"File deleted successfully");
                                else
                                    Log.i(TAG,"Failed to delete the file");

                                DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                                out.writeUTF("1");
                                out.flush();
                                out.close();
                                in.close();
                            } else { // Key is not with this AVD, pass it to the successor

                                try{
                                    Log.i(TAG, "The file is not in this AVD. Search in the successor");
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(successor) *2);

                                    String msgToSend = "deleteWithKey";
                                    Log.i(TAG, "delete - server of a successor sending the key to its successor: " + successor);
                                    DataOutputStream out2 = new DataOutputStream(socket.getOutputStream());
                                    out2.writeUTF(msgToSend);
                                    out2.writeUTF(portNo);
                                    out2.writeUTF(selection);
                                    out2.flush();

                                    DataInputStream in2 = new DataInputStream((socket.getInputStream()));
                                    String ack = in2.readUTF();

                                    if (ack != null ) {
                                        String count = ack;
                                        Log.i(TAG, " " + ack + " deleted from the successor: " + successor );
                                        Log.i(TAG, "inside if - closing the socket at server of successor that connected to its successor");
                                        in2.close();
                                        out2.close();
                                        socket.close();

                                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                                        Log.i(TAG, "server side sent <key,value> to the predecessor who called: ");
                                        out.writeUTF(ack);
                                        out.flush();
                                        out.close();
                                        in.close();
                                    }

                                }catch (UnknownHostException e) {
                                    Log.e(TAG, "ClientTask UnknownHostException");
                                } catch (IOException e) {
                                    Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
                                }
                            }
                            }
                        }




                    else if (msgType.equals("queryWithKey")){
                        String portNo = in.readUTF();
                        String selection = in.readUTF();
                        Log.i(TAG, "At a successors server to query initiated at: " + portNo);
                        if (portNo.equals(myPort)){
                            Log.i(TAG, "Key is not found in any of the AVDs");
                        } else {

                            String keyHash = genHash(selection);
                            boolean isHere = isThisBelongsToMe(keyHash);
                            if (isHere) {
                                Log.i(TAG, "" + selection + " is here in this AVD");
                                Cursor resultCursor = query(null, null, selection, null, null);
//                                Log.i(TAG, "Hello");
                                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                                Log.i(TAG, "keyIdx , valueidx " + keyIndex + "  " + valueIndex);
                                resultCursor.moveToFirst();
                                String returnKey = resultCursor.getString(keyIndex);
                                String returnValue = resultCursor.getString(valueIndex);
                                Log.i(TAG, "Value for key: " + returnKey + " is " + returnValue);
                                String keyValPair = returnKey + "," + returnValue;
                                resultCursor.close();

                                Log.i(TAG, "server side sent <key,value> to the predecessor who called: ");
                                DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                                out.writeUTF(keyValPair);
                                out.flush();
                                out.close();
                                in.close();
                            } else { // Key is not with this AVD, pass it to the successor

                                try{
                                    Log.i(TAG, "The file is not in this AVD. Search in the successor");
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(successor) *2);

                                    String msgToSend = "queryWithKey";
                                    Log.i(TAG, "query - server of a successor sending the key to its successor: " + successor);
                                    DataOutputStream out2 = new DataOutputStream(socket.getOutputStream());
                                    out2.writeUTF(msgToSend);
                                    out2.writeUTF(portNo);
                                    out2.writeUTF(selection);
                                    out2.flush();

                                    DataInputStream in2 = new DataInputStream((socket.getInputStream()));
                                    String ack = in2.readUTF();

                                    if (ack != null ) {
                                        String keyValPair = ack;
                                        Log.i(TAG, "received the keyvaluePair- " + ack + "from the successor: " + successor );
                                        Log.i(TAG, " inside if - closing the socket at server of successor that connected to its successor");
                                        in2.close();
                                        out2.close();
                                        socket.close();

                                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                                        Log.i(TAG, "server side sent <key,value> to the predecessor who called: ");
                                        out.writeUTF(keyValPair);
                                        out.flush();
                                        out.close();
                                        in.close();
                                    }

                                }catch (UnknownHostException e) {
                                    Log.e(TAG, "ClientTask UnknownHostException");
                                } catch (IOException e) {
                                    Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
                                }

                            }
                        }
                    }


                } catch (Exception e) {
                    Log.e(TAG, "Exception caught when trying to listen on port" + e.getMessage());
                } finally {
                    try {
                        if (clientSocket != null) {
                            clientSocket.close();
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "Exception caught closing socket at server:" + e.getMessage());
                    }
                }
                //return null;
            }
        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.i(TAG, "msg received at client " + msgs[0]);

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT0));
                String msgToSend = msgs[0];

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                Log.i(TAG, "Client side sending: " + msgToSend);
                out.writeUTF("portJoin");
                out.writeUTF(msgToSend);
                out.flush();

                DataInputStream in = new DataInputStream((socket.getInputStream()));
                String ack = in.readUTF();

                if (ack.equals("predecessorSuccessorValues")){
                    predecessorHash = in.readUTF();
                    successorHash = in.readUTF();
                    Log.i(TAG, "Received the PredecessorHash: " + predecessorHash + " and SuccessorHash: " + successorHash);

                    correctMySuccessor();
                    correctMyPredecessor();

                    in.close();
                    out.close();
                    socket.close();
                }

                if (ack.equals("ack") ) {
                    Log.i(TAG, "inside if loop - closing the socket at client" );
                    in.close();
                    out.close();
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
            }
            return null;
        }

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        if (selection.equals("@")){
            Log.i(TAG,"Please query @ ");
            String[] columnNames = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor cursor = new MatrixCursor(columnNames);
            for(int i=0;i<fileList.size();i++){
                String filename = fileList.get(i);
                Log.i(TAG, "Quering " + filename);
                String value = null;
                FileInputStream inputStream;
                try {
                    inputStream = getContext().openFileInput(filename);
                    int size = inputStream.available();
                    byte[] buffer = new byte[size-1];
                    inputStream.read(buffer);
                    inputStream.close();
                    value = new String(buffer);
                    Log.i(TAG, "Query value: " + value);
                } catch (Exception e) {
                    Log.e(TAG, "File read failed" + e.getMessage());
                }

                String[] values = {filename, value};
                cursor.addRow(values);
            }
            return cursor;
        }
        else if (selection.equals("*")){
            Log.i(TAG,"Please query * ");
            String[] columnNames = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor cursor = new MatrixCursor(columnNames);
            if (isOnlyOnePresent()){
                for(int i=0;i<fileList.size();i++){
                    String filename = fileList.get(i);
                    Log.i(TAG, "Quering " + filename);
                    String value = "";
                    FileInputStream inputStream;
                    try {
                        inputStream = getContext().openFileInput(filename);
                        int size = inputStream.available();
                        byte[] buffer = new byte[size-1];
                        inputStream.read(buffer);
                        inputStream.close();
                        value = new String(buffer);
                        Log.i(TAG, "Query value: " + value);
                    } catch (Exception e) {
                        Log.e(TAG, "File read failed" + e.getMessage());
                    }

                    String[] values = {filename, value};
                    cursor.addRow(values);
                }
            }
            else {
                String portListStr = getLivePortList();
                Log.i(TAG, "In Query *  Port list String: " + portListStr);
                String[] portList = portListStr.split(",");
                Log.i(TAG, "In Query *  Port list array: " + portList.toString());
                String allKeyValPairsMerged = "";

                for (int i = 0; i < portList.length; i++) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portList[i]) * 2);

                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("queryAll");
                        out.flush();

                        DataInputStream in = new DataInputStream((socket.getInputStream()));
                        String ack = in.readUTF();

                        if (ack != null) {
                            Log.i(TAG, "Received  " + ack + " from " + portList[i]);
                            allKeyValPairsMerged = allKeyValPairsMerged + ack;
                            in.close();
                            out.close();
                            socket.close();
                        }

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException " + e.getMessage() + e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException:" + e.getMessage());
                    }
                }

                Log.i(TAG, "All <k,V> pairs merged: " + allKeyValPairsMerged);
                String[] keyValuePairs = allKeyValPairsMerged.split("&");
                for(int i=0;i<keyValuePairs.length;i++){
                    Log.i(TAG, "All <k,V> pairs in array: " + keyValuePairs[i]);
                }
                for (String KVmerged : keyValuePairs) {
                    String[] keyVal = KVmerged.split(",");
                    if (!keyVal[0].equals("$") || !keyVal[1].equals("$")) {
                        Log.i(TAG, "Key: " + keyVal[0] + " Value: " + keyVal[1]);
                        cursor.addRow(keyVal);
                    }
                }
            }
            return cursor;
        }
        else {           // selection has a file name
            String filename = selection;
            String keyHash = null;
            try {
                keyHash = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            boolean isHere = isThisBelongsToMe(keyHash);
            if (isHere){
                Log.i(TAG, "" + filename + " is in this AVD");
                String value = null;
                FileInputStream inputStream;

                try {
                    // From PA1 code
                    inputStream = getContext().openFileInput(filename);
                    int size = inputStream.available();
                    byte[] buffer = new byte[size-1];
                    inputStream.read(buffer);
                    inputStream.close();
                    value = new String(buffer);
                    Log.i(TAG, "Query value: " + value);
                } catch (Exception e) {
                    Log.e(TAG, "File read failed" + e.getMessage());
                }
                String[] columnNames = {KEY_FIELD, VALUE_FIELD};
                String[] values = {filename, value};
                MatrixCursor cursor = new MatrixCursor(columnNames);
                cursor.addRow(values);
                return cursor;
            } else{   // File is not here. So pass it to the successor
                try{
                Log.i(TAG, "The file is not in this AVD. Search in the successor");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor) *2);

                    String msgToSend = "queryWithKey";
                    Log.i(TAG, "query method sending the key to: " + successor);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(msgToSend);
                    out.writeUTF(myPort);
                    out.writeUTF(selection);
                    out.flush();

                    DataInputStream in = new DataInputStream((socket.getInputStream()));
                    String ack = in.readUTF();

                    if (ack != null ) {
                        String keyValPair = ack;
                        Log.i(TAG, "received the keyvaluepair- " + keyValPair + "from the successor: " + successor );
                        Log.i(TAG, " inside if - closing the socket at query() that connected to its successor");
                        in.close();
                        out.close();
                        socket.close();
                        Log.i(TAG,"Splitting with comma");
                        String[] keyValArray = keyValPair.split(",");

                        String[] columnNames = {KEY_FIELD, VALUE_FIELD};
//                        String[] values = {filename, value};
                        MatrixCursor cursor = new MatrixCursor(columnNames);
                        cursor.addRow(keyValArray);
                        return cursor;
                    }

                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
                }
            }

        }
        return null;
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

    public String[] computePredecessorSuccessor(String portNo){
        int size = arrayList.size();
        if (size == 0) return null;
        if (size == 1) {
            try {
                String portNoHash = genHash(portNo);
                String[] output = {portNoHash,portNoHash};
                return output;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else {
            try {
                String portNoHash = genHash(portNo);
                int index = arrayList.indexOf(portNoHash);
                int preIndex;
                int sucIndex;
                if (index == 0) {
                    preIndex = size - 1;
                    sucIndex = index + 1;
                }else if (index == size -1){
                    sucIndex = 0;
                    preIndex = index - 1;
                }else {
                    preIndex = index - 1;
                    sucIndex = index + 1;
                }
                String[] output = {arrayList.get(preIndex),arrayList.get(sucIndex)};
                return output;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public void correctMySuccessor(){
        successor = map.get(successorHash);
        try{
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(successor) *2);

            String msgToSend = "updateYourPredecessor";
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            Log.i(TAG, "Client side sending: " + msgToSend);
            out.writeUTF(msgToSend);
            out.writeUTF(myPortHash);
            out.flush();

            DataInputStream in = new DataInputStream((socket.getInputStream()));
            String ack = in.readUTF();

            if (ack.equals("ack") ) {
                Log.i(TAG, "inside if loop - closing the socket at correctMySuccessor()" );
                in.close();
                out.close();
                socket.close();
            }
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
        }
    }

    public void correctMyPredecessor(){
        predecessor = map.get(predecessorHash);
        try{
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(predecessor) *2);

            String msgToSend = "updateYourSuccessor";
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            Log.i(TAG, "Client side sending: " + msgToSend);
            out.writeUTF(msgToSend);
            out.writeUTF(myPortHash);
            out.flush();

            DataInputStream in = new DataInputStream((socket.getInputStream()));
            String ack = in.readUTF();

            if (ack.equals("ack") ) {
                Log.i(TAG, "inside if loop - closing the socket at correctMyPredecessor()" );
                in.close();
                out.close();
                socket.close();
            }

        }catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
        }
    }

    public boolean isOnlyOnePresent(){
        if (predecessor == null && successor == null)
            return true;
        else
            return false;
    }

    public boolean isThisBelongsToMe(String keyHash){
        Log.i(TAG, "In isThisBelongsToMe() method");
        if (isOnlyOnePresent()){
            Log.i(TAG, "Only one node is present in the network ");
            return true;
        }

        if (predecessorHash.compareTo(myPortHash) > 0){     //This checks if the node is the first node in the list
            Log.i(TAG,"This is the first node in the list");
            if ((keyHash.compareTo(myPortHash) <=0) || keyHash.compareTo(predecessorHash) > 0)
                return true;
            else
                return false;
        }
        else {         // This is not the first node in the list
            if (keyHash.compareTo(myPortHash) <=0 && keyHash.compareTo(predecessorHash) >0)
                return true;
            else
                return false;
        }
    }

    public String getLivePortList(){
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(REMOTE_PORT0));

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("givePortList");
            out.flush();

            DataInputStream in = new DataInputStream((socket.getInputStream()));
            String portListStr = in.readUTF();

            if (portListStr != null){
                in.close();
                out.close();
                socket.close();
                return portListStr;
            }

        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
        }
        return null;
    }
}

// succesor = 5554
// succesorHash = aargawgllknlnnaet
// myPort = 5554
// myPortHash = asvawrgwgr
