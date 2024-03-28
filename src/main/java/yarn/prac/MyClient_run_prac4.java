package yarn.prac;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import yarn.MyClient;

public class MyClient_run_prac4 {
	private static final Log LOG = LogFactory.getLog(MyClient.class);
	private YarnClient yarnClient;
	private int amMemory = 10;
	private int amVCores = 1;
	private int amPriority = 0;
	private String appName = "";
	private String amQueue = "";
	
	public boolean run() throws IOException, YarnException {

		// 0) 얀클라이언트 시작
		yarnClient.start();
		
	    // 1) AP ID

	    // 2) 최대 메모리 처리
	    // 3) 최대 코어 처리
	    // 4) AP 이름 설정
	    // 5) 리소스 설정
	    
	    // 6) 우선순위 설정

	    // 7) 큐설정

	    // *8) 컨테이너 스팩 설정

	    // 9) 최종 전송

	    // Monitor the application
	    return false;
	  }
}
