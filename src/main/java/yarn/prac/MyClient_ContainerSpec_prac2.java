package yarn.prac;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import yarn.Constants;
import yarn.MyClient;

public class MyClient_ContainerSpec_prac2 {
	  private Configuration conf;
	  private static final Log LOG = LogFactory.getLog(MyClient.class);
	  private String appMasterJarPath = "";
	  private int amMemory = 10;
	  private int containerMemory = 10;
	  private int containerVirtualCores = 1;
	  private int numContainers = 1;
	  private int requestPriority = 0;
	  
	  private ContainerLaunchContext getAMContainerSpec(int appId) throws IOException, YarnException {
		    // 1) 컨테이너 시작 설정
		  	ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		  	FileSystem fs = FileSystem.get(conf);
		  	
		    // 2) HDFS에 리소스 저장
		  	Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		  	addToLocalResources();
		  	
		    // 3) 로컬 리소스 정보 설정
		  	amContainer.setLocalResources(localResources);

		    // 4) 환경변수 설정
		  	amContainer.setEnvironment(null);

		    // 5) 커맨드라인 설정
		  	List<String> commands = new ArrayList<String>();
		  	amContainer.setCommands(commands);
		  	
		    return amContainer;
		  }
}
