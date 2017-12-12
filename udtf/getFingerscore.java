package org.udtf;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.UDFException;
@Resolve({"bigint,string,string,string->bigint,string,bigint"})
public class getFingerscore extends UDTF {
  @Override
  public void process(Object[] args) throws UDFException {
	    Long row_id = (Long) args[0];
	  	String shop_id = (String) args[1];
	  	String wifi_infos = (String) args[2];
	  	String key_values = (String) args[3];
	    long score=0;
	  	if(key_values!=null&&wifi_infos!=null){
		    Map <String,Integer> wifi_list=new HashMap();
		    for(String wifi : wifi_infos.split(";")){
		    	String[] info=wifi.split("\\|");
		    	if(info[1].equals("null")) continue;
		    	wifi_list.put(info[0],Integer.valueOf(info[1]));
		    }
		    
		    Map <String,HashSet<String>> finger = new HashMap();
		    for(String bssid1 : wifi_list.keySet()){
		    	for(String bssid2 : wifi_list.keySet()){
		    		if(wifi_list.get(bssid1)-wifi_list.get(bssid2)>=6){
		    			if(!finger.containsKey(bssid1)){
		    				finger.put(bssid1, new HashSet());
		    			}
		    			finger.get(bssid1).add(bssid2);
		    		}
		    	}
		    }
		    
		    Map <String,HashSet<String>> fingerbase = new HashMap();
		    for(String kv : key_values.split(";")){
		    	if(kv.split(":").length<=1) continue;
		    	String key=kv.split(":")[0];
		    	if(!fingerbase.containsKey(key)){
		    		fingerbase.put(key,new HashSet());
		    	}
		    	for(String value : kv.split(":")[1].split(",")){
		    		fingerbase.get(key).add(value);
		    	}
		    }
		    for(String key :finger.keySet()){
		    	if(!fingerbase.containsKey(key)) continue;
		    	for(String value : finger.get(key)){
		    		if(fingerbase.get(key).contains(value)) score+=1;
		    	}
		    }
	  	}
	    forward(row_id,shop_id,score);
//	  	System.out.println(score);
  }
  @Test
  public void test() {
  	Object[] tmp = new Object [5];
  	tmp[0]=(long)123;
  	tmp[1]="s_21323";
  	tmp[2]="s_21323";
  	tmp[3] = "baaid|null|True;id|-88|True;issd|-66|True#id|-44|True;ggg|-55|True;ccc|-99|True";
  	tmp[4] ="b_34351649:b_27349942,b_34351772,b_1581348,b_32364874,b_34352149;b_34351648:b_27349942,b_34351772,b_1581348,b_32364874,b_34352149;b_45728745:b_27349942;";
  	try {
  		process(tmp);
  	} catch (UDFException e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
  	}
  }
}