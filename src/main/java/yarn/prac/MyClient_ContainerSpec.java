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

public class MyClient_ContainerSpec {
	  private Configuration conf;
	  private static final Log LOG = LogFactory.getLog(MyClient.class);
	  private String appMasterJarPath = "";
	  private int amMemory = 10;
	  private int containerMemory = 10;
	  private int containerVirtualCores = 1;
	  private int numContainers = 1;
	  private int requestPriority = 0;
	  
	  private ContainerLaunchContext getAMContainerSpec(int appId) throws IOException, YarnException {
		    // Set up the container launch context for the application master
		    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		    FileSystem fs = FileSystem.get(conf);

		    // set local resources for the application master
		    // local files or archives as needed
		    // In this scenario, the jar file for the application master is part of the local resources
		    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		    LOG.info("Copy App Master jar from local filesystem and add to local environment");
		    // Copy the application master jar to the filesystem
		    // Create a local resource to point to the destination jar path
		    addToLocalResources(fs, appMasterJarPath, Constants.AM_JAR_NAME, appId,
		        localResources, null);

		    // Set local resource info into app master container launch context
		    amContainer.setLocalResources(localResources);

		    // Set the env variables to be setup in the env where the application master will be run
		    LOG.info("Set the environment for the application master");
		    amContainer.setEnvironment(getAMEnvironment(localResources, fs));

		    // Set the necessary command to execute the application master
		    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

		    // Set java executable command
		    LOG.info("Setting up app master command");
		    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		    // Set Xmx based on am memory size
		    vargs.add("-Xmx" + amMemory + "m");
		    // Set class name
		    vargs.add("com.wikibooks.hadoop.yarn.examples.MyApplicationMaster");
		    // Set params for Application Master
		    vargs.add("--container_memory " + String.valueOf(containerMemory));
		    vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
		    vargs.add("--num_containers " + String.valueOf(numContainers));
		    vargs.add("--priority " + String.valueOf(requestPriority));
		    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		    // Get final command
		    StringBuilder command = new StringBuilder();
		    for (CharSequence str : vargs) {
		      command.append(str).append(" ");
		    }

		    LOG.info("Completed setting up app master command " + command.toString());
		    List<String> commands = new ArrayList<String>();
		    commands.add(command.toString());
		    amContainer.setCommands(commands);

		    return amContainer;
		  }
}
