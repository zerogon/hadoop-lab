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

public class MyClient_run {
	private static final Log LOG = LogFactory.getLog(MyClient.class);
	private YarnClient yarnClient;
	private int amMemory = 10;
	private int amVCores = 1;
	private int amPriority = 0;
	private String appName = "";
	private String amQueue = "";
	
	public boolean run() throws IOException, YarnException {

	    LOG.info("Running Client");
	    yarnClient.start();

	    // Get a new application id
	    YarnClientApplication app = yarnClient.createApplication();
	    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

	    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
	    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

	    // A resource ask cannot exceed the max.
	    if (amMemory > maxMem) {
	      LOG.info("AM memory specified above max threshold of cluster. Using max value."
	          + ", specified=" + amMemory
	          + ", max=" + maxMem);
	      amMemory = maxMem;
	    }

	    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
	    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

	    if (amVCores > maxVCores) {
	      LOG.info("AM virtual cores specified above max threshold of cluster. "
	          + "Using max value." + ", specified=" + amVCores
	          + ", max=" + maxVCores);
	      amVCores = maxVCores;
	    }

	    // set the application name
	    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
	    ApplicationId appId = appContext.getApplicationId();

	    appContext.setApplicationName(appName);
	    // Set up resource type requirements
	    // For now, both memory and vcores are supported, so we set memory and
	    // vcores requirements
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(amMemory);
	    capability.setVirtualCores(amVCores);
	    appContext.setResource(capability);

	    // Set the priority for the application master
	    Priority pri = Records.newRecord(Priority.class);
	    pri.setPriority(amPriority);
	    appContext.setPriority(pri);

	    // Set the queue to which this application is to be submitted in the RM
	    appContext.setQueue(amQueue);

	    // Set the ContainerLaunchContext to describe the Container ith which the ApplicationMaster is launched.
	    appContext.setAMContainerSpec(getAMContainerSpec(appId.getId()));

	    // Submit the application to the applications manager
	    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
	    // Ignore the response as either a valid response object is returned on success
	    // or an exception thrown to denote some form of a failure
	    LOG.info("Submitting application to ASM");

	    yarnClient.submitApplication(appContext);

	    // Monitor the application
	    return monitorApplication(appId);
	  }
}
