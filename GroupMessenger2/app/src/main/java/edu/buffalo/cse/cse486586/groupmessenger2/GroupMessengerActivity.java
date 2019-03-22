package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static final String[] remotePort = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
//    public int msgSeq = 0;
    public float msgSeq = 0;
    public int msgCounter = 0;
    //public int msgSentCount = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        // Below code taken from PA1
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final Button sendButton = (Button) findViewById(R.id.button4);
        final EditText editText1 = (EditText) findViewById(R.id.editText1);
        sendButton.setOnClickListener(new View.OnClickListener(){
            public void onClick(View view) {
                String msg = editText1.getText().toString() + "\n";
                editText1.setText(""); // This is one way to reset the input box.
//                tv.append("\t" + msg); // This is one way to display a string.
                Log.i(TAG, "CLIENT: MSG read from emulator: " + msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private static final String KEY_FIELD = "key";
        private static final String VALUE_FIELD = "value";

        public final ContentResolver mContentResolver = getContentResolver();
        public final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

        public PriorityQueue<MessageClass> pQueue = new PriorityQueue<MessageClass>(50, new MsgComparator());
        public List<MessageClass> arrayList = new ArrayList<MessageClass>();

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket clientSocket = null;
            while (true) {
                try {
                    clientSocket = serverSocket.accept();
                    Log.i(TAG, "serverSocket accepted ");
                    DataInputStream in = new DataInputStream((clientSocket.getInputStream()));
                    String msgId = in.readUTF();
                    Log.i(TAG, "Msg Id received from client: " + msgId);
                    String msgContent = in.readUTF();
                    Log.i(TAG, "Content received from client: " + msgContent);
                    String flag = in.readUTF();
                    Log.i(TAG, "Flag reveived from client:  " + flag);

                    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

                    if (flag.equals("msg")) {
                        String serverPortNoStr = in.readUTF();
                        Log.i(TAG, "Server Port number:  " + serverPortNoStr);
                        float serverPortNo = Float.parseFloat(serverPortNoStr);
                        float decimal = serverPortNo/100000;

                        String msgToPublish = msgContent;
//                        int proposedNum;
                        float proposedNum;
                        proposedNum = msgSeq++ + 1;
                        proposedNum = proposedNum + decimal;
                        Log.i(TAG, "server side ack sent: ProposedNum: " + proposedNum);

                        out.writeUTF(msgId);
                        out.writeUTF(Float.toString(proposedNum));
                        out.flush();
                        out.close();

                        Log.i(TAG, "Msg Id server-side: " + msgId);
                        Log.i(TAG, "MSG to store in Queue in server-side: " + msgToPublish);
                        MessageClass messageObj = new MessageClass();
                        messageObj.id = msgId;
                        messageObj.msg = msgToPublish;


                        //pQueue.add(messageObj);
                        ////////////////////////////////
                        arrayList.add(messageObj);
                        Log.i(TAG, "Message Object added to queue: " + messageObj);
                        Collections.sort(arrayList,MessageClass.mComparator);

                        /////////////////////
                        /////publishProgress(msgToPublish, "publish this as well please");

                        in.close();
                        out.flush();
                        out.close();
                        //clientSocket.close();
                    } else{
                        String finalizedSeqNum = msgContent;
                        Log.i(TAG,"Server recieved finalizd seq number: " + finalizedSeqNum);
                        msgSeq = Math.max(msgSeq , Math.round(Float.parseFloat(finalizedSeqNum)));
                        MessageClass temp;
                        for (int i = 0;i < arrayList.size(); i++) {
                            temp = arrayList.get(i);
                            if (temp.getId().equals(msgId)){
                                temp.seqNo = Float.parseFloat(finalizedSeqNum);
                                temp.deliverable = true;
                                break;
                            }
                        }
                        Collections.sort(arrayList,MessageClass.mComparator);
                        for (int i = 0; i<arrayList.size();i++){
                            Log.v(TAG, "ARRAYLIST: " + i + " " + arrayList.get(i).toString());
                        }

                        if (arrayList.get(0).deliverable != false){
                            for(int i =0; i < arrayList.size(); i++) {
                                temp = arrayList.get(i);
                                if (temp.deliverable != false) {
                                    Log.i(TAG, "ARRAYLIST: Publishing: " + temp.toString());
                                    publishProgress(temp.getMsg(), Float.toString(temp.getSeqNo()));
                                }
                                else
                                    break;
                            }
                        }

                        // Removing the element one by one for all the elements that are delivered in the previous loop
                        //if (arrayList.get(0).deliverable != false){
                            while(!arrayList.isEmpty()){
                                temp = arrayList.get(0);
                                if (temp.deliverable != false) {
                                    Log.i(TAG, "ARRAYLIST: Removing the msg from queue. " + temp.toString());
                                    arrayList.remove(0);
                                }
                                else
                                    break;
                            }
                        //}
                        Collections.sort(arrayList,MessageClass.mComparator);


                        //DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                        //pQueue.remove()
                        Log.i(TAG, "Server sent Id and ACK " );
                        out.writeUTF(msgId);
                        out.writeUTF("ack");

                        out.flush();
                        out.close();
                        in.close();
                    }

                } catch (Exception e) {
                    Log.e(TAG, "Exception caught when trying to listen on port" + e.toString());
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

//                Iterator<MessageClass> itr = pQueue.iterator();
//                MessageClass temp = new MessageClass();
//                while (itr.hasNext()) {
//                    //MessageClass temp = (MessageClass)itr.next();
//                    temp = itr.next();
//                    System.out.println(temp.getMsg());
//                    System.out.println(temp.getId());
//                    // System.out.println(itr.next().getId());
//
//                }

            }
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();

            Log.i(TAG, "SERVER: Msg writing in content provider: " + strReceived);
            Log.i(TAG, "SERVER: Finalized sequence number: " + strings[1]);
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\t\n");

            ///////////
//            Iterator<MessageClass> itr = pQueue.iterator();
//            MessageClass temp = new MessageClass();
//            while (itr.hasNext()) {
//                //MessageClass temp = (MessageClass)itr.next();
//                temp = itr.next();
//                System.out.println(temp.getMsg());
//                System.out.println(temp.getId());
//                // System.out.println(itr.next().getId());
//
//            }
            ///////////
            /*
             * The following code creates a ContentValues and puts the msgSeq as the key and
             * msg string as the value to the ContentProvider.
             *
             * The code is referred from the OnPTestClickListener java file.
             */

            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, Integer.toString(msgCounter));
//            cv.put(KEY_FIELD, strings[1]);
            cv.put(VALUE_FIELD, strReceived);

            try {
                mContentResolver.insert(mUri, cv);
//                msgSeq++;
                msgCounter ++;
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {
        private List<Float> proposedList = new ArrayList<Float>();
        @Override
        protected Void doInBackground(String... msgs) {

            Log.i(TAG, "CLIENT: MSG that client is going to send " + msgs[0]);
            Log.i(TAG, "CLIENT: Client's Port number: " + msgs[1]);
            Log.i(TAG, "CLIENT: MSG sequence: " + msgSeq);

            String msgId = Float.toString(msgSeq) + msgs[1] ;
//            msgSeq ++;

            Log.i(TAG, "CLIENT: Sending MSG ID " + msgId + " MSG: " + msgs[0] + " Flag: " + "msg");

            try {
                for (int i = 0; i < remotePort.length; i++) {
                    Log.i(TAG, "CLIENT: Iteration number: " + remotePort[i]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort[i]));
//                    String msgToSend = msgs[0];
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
//                    ObjectOutputStream out1 = new ObjectOutputStream(out);
//                    out1.writeObject(messageObj);
//                    out1.flush();
                    out.writeUTF(msgId);
                    out.writeUTF(msgs[0]);
                    out.writeUTF("msg");
                    out.writeUTF(remotePort[i]);

                    DataInputStream in = new DataInputStream((socket.getInputStream()));
//                    ObjectInputStream in1 = new ObjectInputStream(in);
//                    String ack = in.readUTF();
//                    SequenceProposalClass ackObj = (SequenceProposalClass) in1.readObject();
                    String recievedMsgId = in.readUTF();
                    String recievedProposal = in.readUTF();
                    Log.i(TAG, "CLIENT: Response received: msg id: " + recievedMsgId + "Proposed Seq No: " + recievedProposal);

                    if (recievedMsgId.equals(msgId)){
                        Log.i(TAG, "CLIENT: Proposal received from server: "+ recievedProposal);
//                        int proposedNum  = Integer.parseInt(recievedProposal);
                        float proposedNum  = Float.parseFloat(recievedProposal);
                        proposedList.add(proposedNum);
                        if (proposedNum != -1){
//                        Log.i(TAG, "inside if loop " );
                            in.close();
                            out.close();
                            socket.close();
                        }
                    }

                    //Log.i(TAG, "ack received in the client side: msg id: " + ackObj.id + "Proposed Seq No: " + ackObj.proposedNum);
//                    proposedList.add(ackObj.proposedNum);


//                    if (ackObj.proposedNum != -1 ) {

                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
            } //catch (ClassNotFoundException e){
                //Log.e(TAG, "Class Not found exception");
            //}

            Log.i(TAG,"CLIENT: List of all proposed Seq number: " + proposedList);
            Float max = Collections.max(proposedList);
            Log.i(TAG,"CLIENT: Max of all proposed Seq number: " + max);

//            SequenceProposalClass sendMax = new SequenceProposalClass();
//            sendMax.id = msgId;
//            sendMax.proposedNum = max;

            try {
                for (int i = 0; i < remotePort.length; i++) {
                    Log.i(TAG, "CLIENT: Iteration number for sending finalized seqNo: " + remotePort[i]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort[i]));
//                    String msgToSend = msgs[0];

                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(msgId);
                    out.writeUTF(Float.toString(max));
                    out.writeUTF("num");

                    DataInputStream in = new DataInputStream((socket.getInputStream()));
                    String recievedMsgId = in.readUTF();
                    String ack = in.readUTF();

                    Log.i(TAG, "CLIENT: Ack received: Msg id:  "+ recievedMsgId + "ACK: "+ack);

                    if (recievedMsgId.equals(msgId) && ack.equals("ack") ){
//                        Log.i(TAG, "inside if loop " );
                        in.close();
                        out.close();
                        socket.close();
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
            }

//            try {
//                for (int i = 0; i < remotePort.length; i++) {
//                    Log.i(TAG, "Iteration number in client side: " + remotePort[i]);
//                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                            Integer.parseInt(remotePort[i]));
////                        String msgToSend = msgs[0];
//
//
//                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
//                    ObjectOutputStream out1 = new ObjectOutputStream(out);
//                    out1.writeObject(sendMax);
//                    out1.flush();
//
//                    DataInputStream in = new DataInputStream((socket.getInputStream()));
//                    ObjectInputStream in1 = new ObjectInputStream(in);
////                        String ack = in.readUTF();
//                    String ack = (String) in1.readObject();
//
//
//
//
//                    if (ack.equals("ack") ) {
////                            Log.i(TAG, "inside if loop " );
//                        in1.close();
//                        out1.close();
//                        socket.close();
//                    }
//                }
//            } catch (UnknownHostException e) {
//                Log.e(TAG, "ClientTask UnknownHostException");
//            } catch (IOException e) {
//                Log.e(TAG, "ClientTask socket IOException:"+ e.toString());
//            } catch (ClassNotFoundException e){
//                Log.e(TAG, "Class Not found exception");
//            }

            return null;
        }
    }
}
