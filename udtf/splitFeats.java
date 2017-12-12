package org.udtf;
import org.junit.Test;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.UDFException;
@Resolve({"string->string"})
public class splitFeats extends UDTF {
  @Override
  public void process(Object[] args) throws UDFException {
	    String input = (String) args[0];
	    String[] test = input.split(",");
	    for (int i = 0; i < test.length; i++) {
	        forward(test[i].replace(" ",""));
	    }
  }
  @Test
  public void test() {
  	Object[] tmp = new Object [2];
  	tmp[0] = "sd, as, gr";
  	try {
  		process(tmp);
  	} catch (UDFException e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
  	}
  }
}