package yarn.prac;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import yarn.Constants;

public class MyClient_getAMEnvironment {
	
	  private Configuration conf;
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
