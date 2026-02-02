package qsc.dbrule;

import java.util.Vector;

public class Constants {
    public Vector<String> constant1 = new Vector<String>();
    public Vector<String> constant2 = new Vector<String>();

    public Constants(){
    }
    public void addConstant(String constant, int pos){
        if(pos == 1){
            constant2.add(constant);
        }else{
            constant1.add(constant);
        }
        int len1 = constant1.size();
        int len2 = constant2.size();
        if(len1>0 && len2>0){
        }else if(len1>0||len2>0){
        }
    }
}