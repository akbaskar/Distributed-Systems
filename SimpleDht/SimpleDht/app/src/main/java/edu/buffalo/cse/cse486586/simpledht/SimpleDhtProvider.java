package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static String predecessor = "";
    private static String successor = "";
    private static String predecessorHash = "";
    private static String successorHash = "";

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



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String filename = selection;
        System.out.println("Delete this file: " + filename);

//        File file = new File(filename);
//        if(file.delete())
        if(getContext().deleteFile(filename))
        {
            System.out.println("File deleted successfully");
        }
        else
        {
            System.out.println("Failed to delete the file");
        }
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

        // Below code is taken from PA1
        String filename = key;
        String string = val + "\n";
        FileOutputStream outputStream;

        try {
            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(string.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        Log.v("insert", values.toString());
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

                        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        Log.i(TAG, "server side ack sent: " + "ack");
                        out.writeUTF("ack");
                        out.flush();
                        out.close();
                        in.close();
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Exception caught when trying to listen on port");
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
        String filename = selection;
        String value = null;
        FileInputStream inputStream;

        Log.i(TAG, "Query filename: " + filename);

        try {
            inputStream = getContext().openFileInput(filename);
            int size = inputStream.available();
//            Log.i(TAG, "size: " + size);
            byte[] buffer = new byte[size-1];
            inputStream.read(buffer);
            inputStream.close();
            value = new String(buffer);
            Log.i(TAG, "Query value: " + value);
        } catch (Exception e) {
            Log.e(TAG, "File read failed" + e.getMessage());
        }
        String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        String[] values = {selection, value};
        MatrixCursor cursor = new MatrixCursor(columnNames);
        cursor.addRow(values);
//        Log.v("query", selection);
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
}

// succesor = 5554
// succesorHash = aargawgllknlnnaet
// myPort = 5554
// myPortHash = asvawrgwgr
