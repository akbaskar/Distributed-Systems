package edu.buffalo.cse.cse486586.groupmessenger2;


import java.util.Comparator;

public class MessageClass {
    public String msg;
    public String id;
    public float seqNo = -1;
    public boolean deliverable = false;

    public String getMsg(){
        return msg;
    }
    public String getId(){
        return id;
    }
    public float getSeqNo(){
        return seqNo;
    }
    public void setSeqNo(int a){
        this.seqNo = a;
    }
    public String toString(){
        return ("MSG: " + msg + "ID: " + id + " Seq No: " + seqNo + " Deliverable: " + deliverable);
    }

    public static Comparator<MessageClass> mComparator = new Comparator<MessageClass>() {

        public int compare(MessageClass s1, MessageClass s2) {
//            Float a = Float.parseFloat(s1.id);
//            Float b = Float.parseFloat(s2.id);
//            //ascending order
//            if (a > b) return  1;
//            else if (a < b) return -1;
//            else  return 0;
//            return a - b;

            if (s1.seqNo > s2.seqNo) return 1;
            else if (s1.seqNo < s2.seqNo) return -1;
            else return 0;

        }
    };
}

class MsgComparator implements Comparator<MessageClass> {



    public int compare(MessageClass s1, MessageClass s2) {
        int a = Integer.parseInt(s1.id);
        int b = Integer.parseInt(s2.id);
        if (a < b)
            return -1;
        else if (a > b)
            return 1;
        return 0;
    }
}
