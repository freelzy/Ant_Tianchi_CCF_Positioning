package org.udtf;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.UDFException;
@Resolve({"string,string,string->string,string,string"})
public class wifiFingerbase extends UDTF {
  @Override
  public void process(Object[] args) throws UDFException {
	  	String mall_id = (String) args[0];
	  	String shop_id = (String) args[1];
	  	String wifi_all = (String) args[2];
	  	Map<String,HashSet<String>> fingerbase = new HashMap();
	  	
	  	for(String wifi_infos : wifi_all.split("#")){
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
		    
		    for(String key : finger.keySet()){
		    	if(!fingerbase.containsKey(key)){
		    		fingerbase.put(key, new HashSet());
				}
		    	for(String value : finger.get(key)){
		    		fingerbase.get(key).add(value);
		    	}
		    }
	  	}
	  	String res=new String();
	  	for(String key : fingerbase.keySet()){
	  		res+=key;
	  		res+=":";
	  		int i=0;
	  		for(String value : fingerbase.get(key)){
	  			if(i==0) res+=value;
	  			else{
	  				res+=",";
	  				res+=value;
	  			}
	  			i++;
	  		}
	  		res+=";";
	  	}
	  	forward(mall_id,shop_id,res);
//	  	System.out.println(shop_id+" "+res);
  }
  @Test
  public void test() {
  	Object[] tmp = new Object [2];
  	tmp[0]="s_21323";
  	tmp[1] = "baaid|null|True;id|-88|True;issd|-66|True#id|-44|True;ggg|-55|True;ccc|-99|True";
  	try {
  		process(tmp);
  	} catch (UDFException e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
  	}
  }
}