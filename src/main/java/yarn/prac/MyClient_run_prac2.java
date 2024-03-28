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

public class MyClient_run_prac2 {
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
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse =  app.getNewApplicationResponse();

	    // 2) 최대 메모리 처리
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
	    // 3) 최대 코어 처리
	    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
	    // 4) AP 이름 설정
	    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
	    appContext.setApplicationName(appName);
	    // 5) 리소스 설정
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(amVCores);
	    capability.setVirtualCores(amVCores);
	    
	    appContext.setResource(capability);
	    
	    // 6) 우선순위 설정
	    Priority pri = Records.newRecord(Priority.class);
	    pri.setPriority(amPriority);
	    
	    appContext.setPriority(pri);

	    // 7) 큐설정
	    appContext.setQueue(amQueue);
	    // *8) 컨테이너 스팩 설정
	    appContext.setAMContainerSpec(null);
	    
	    // 9) 최종 전송
	    yarnClient.submitApplication(appContext);
	    
	    // Monitor the application
	    return monitorApplication(appId);
	  }
}
