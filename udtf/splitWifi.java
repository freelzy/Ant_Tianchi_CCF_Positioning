package org.udtf;
import org.junit.Test;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.UDFException;
@Resolve({"bigint,string->bigint,string,string,string"})
public class splitWifi extends UDTF {
  @Override
  public void process(Object[] args) throws UDFException {
	  	Long row_id = (Long) args[0];
	    String input = (String) args[1];
	    String[] test = input.split(";");
	    for (int i = 0; i < test.length; i++) {
	        String[] result = test[i].split("\\|");
	        forward(row_id,result[0],result[1],result[2]);
	    }
  }
  @Test
  public void test() {
  	Object[] tmp = new Object [2];
  	tmp[0]=(long)123;
  	tmp[1] = "baaid|null|True;baaid|44|True;";
  	try {
  		process(tmp);
  	} catch (UDFException e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
  	}
  }
}