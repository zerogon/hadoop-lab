package yarn.prac;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import yarn.Constants;
import yarn.MyClient;

public class MyClient_ContainerSpec_prac3 {
	  private Configuration conf;
	  private static final Log LOG = LogFactory.getLog(MyClient.class);
	  private String appMasterJarPath = "";
	  private int amMemory = 10;
	  private int containerMemory = 10;
	  private int containerVirtualCores = 1;
	  private int numContainers = 1;
	  private int requestPriority = 0;
	  private String appName = "";
	  
	  private ContainerLaunchContext getAMContainerSpec(int appId) throws IOException, YarnException {
		  // 개요: 외부 리소스 저장, 1) 로컬환경설정, 2) 환경변수, 3) 커맨드 라인
		    // 1) 컨테이너 시작 설정

		    // 2) HDFS에 리소스 저장
		  
		    // 3) 로컬 리소스 정보 설정

		    // 4) 환경변수 설정
		    // 5) 커맨드라인 설정
		  	
		  	
		    return amContainer;
		  }
	  
	  private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, int appId,
				Map<String, LocalResource> localResources, String resources) throws IOException {
			String suffix = appName + "/" + appId + "/" + fileDstPath;
			Path dst = new Path(fs.getHomeDirectory(), suffix);
			if (fileSrcPath == null) {
				FSDataOutputStream ostream = null;
				try {
					ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
					ostream.writeUTF(resources);
				} finally {
					IOUtils.closeQuietly(ostream);
				}
			} else {
				fs.copyFromLocalFile(new Path(fileSrcPath), dst);
			}
			FileStatus scFileStatus = fs.getFileStatus(dst);
			LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
					LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
					scFileStatus.getModificationTime());
			localResources.put(fileDstPath, scRsrc);
		}
	  private Map<String, String> getAMEnvironment(Map<String, LocalResource> localResources
		      , FileSystem fs) throws IOException{
		    Map<String, String> env = new HashMap<String, String>();

		    // Set ApplicationMaster jar file
		    LocalResource appJarResource = localResources.get(Constants.AM_JAR_NAME);
		    Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
		    FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
		    long hdfsAppJarLength = hdfsAppJarStatus.getLen();
		    long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

		    env.put(Constants.AM_JAR_PATH, hdfsAppJarPath.toString());
		    env.put(Constants.AM_JAR_TIMESTAMP, Long.toString(hdfsAppJarTimestamp));
		    env.put(Constants.AM_JAR_LENGTH, Long.toString(hdfsAppJarLength));

		    // Add AppMaster.jar location to classpath
		    // At some point we should not be required to add
		    // the hadoop specific classpaths to the env.
		    // It should be provided out of the box.
		    // For now setting all required classpaths including
		    // the classpath to "." for the application jar
		    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
		        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		    for (String c : conf.getStrings(
		        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
		        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
		      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
		      classPathEnv.append(c.trim());
		    }
		    env.put("CLASSPATH", classPathEnv.toString());

		    return env;
		  }
}
