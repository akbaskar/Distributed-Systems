package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

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

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
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
        final TextView tv = (TextView) findViewById(R.id.textView1);
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

        // https://www.youtube.com/watch?v=dFlPARW5IX8
        final Button sendButton = (Button) findViewById(R.id.button4);
        final EditText editText1 = (EditText) findViewById(R.id.editText1);
        sendButton.setOnClickListener(new View.OnClickListener(){
            public void onClick(View view) {
                String msg = editText1.getText().toString() + "\n";
                editText1.setText(""); // This is one way to reset the input box.
//                tv.append("\t" + msg); // This is one way to display a string.
                Log.i(TAG, "Read from emulator: " + msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, null);
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
        public int msgSeq = 0;
        public final ContentResolver mContentResolver = getContentResolver();
        public final Uri  mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");



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
                    Log.i(TAG, "serverSocket accept ");
                    DataInputStream in = new DataInputStream((clientSocket.getInputStream()));
                    String msgToPublish = in.readUTF();

//                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//                Log.i(TAG, "server side ack sent: " + "ack");
//                out.println("ack");
                    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                    Log.i(TAG, "server side ack sent: " + "ack");
                    out.writeUTF("ack");
                    out.flush();
                    out.close();


//                while ((msgToPublish ) != null) {
                    Log.i(TAG, "Msg to publish in server-side: " + msgToPublish);
                    publishProgress(msgToPublish);
//                }

                    Log.i(TAG, "here ");

                    in.close();
                    //out.flush();
                    //out.close();
                    //clientSocket.close();
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


        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            Log.i(TAG, "SERVER: Msg received after publish: " + strReceived);
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\t\n");

            /*
             * The following code creates a ContentValues and puts the msgSeq as the key and
             * msg string as the value to the ContentProvider.
             *
             * The code is referred from the OnPTestClickListener java file.
             */

            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, Integer.toString(msgSeq));
            cv.put(VALUE_FIELD, strReceived);

            try {
                mContentResolver.insert(mUri, cv);
                msgSeq ++;
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.i(TAG, "msg received at client " + msgs[0]);

                for (int i = 0; i < remotePort.length; i++) {
                    Log.i(TAG, "iteration number in client side: " + remotePort[i]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort[i]));
                    String msgToSend = msgs[0];

                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Log.i(TAG, "Client side sending: " + msgToSend);
                    out.writeUTF(msgToSend);
                    out.flush();

                    DataInputStream in = new DataInputStream((socket.getInputStream()));
                    String ack = in.readUTF();


                    //Log.i(TAG, "ack received in the client side: " + ack);

                    if (ack.equals("ack") ) {
                        Log.i(TAG, "inside if loop " );
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

            return null;
        }
    }
}
