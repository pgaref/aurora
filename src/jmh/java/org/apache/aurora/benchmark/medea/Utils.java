package org.apache.aurora.benchmark.medea;

/**
 * Created by pgaref on 16/01/17.
 */

import java.util.ArrayList;
import org.apache.hadoop.mapred.gridmix.services.ServiceJobStory;
import org.apache.hadoop.mapred.gridmix.services.ServiceJobTraceProducer;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Utils {

  protected static final int GB = 1024;

  public static int generateServiceJobs(YarnConfiguration rmConf,
      int numServiceTasks, Map<ApplicationId, ConstraintDefinition> appCMap,
      Map<ApplicationId, List<ResourceRequest>> appReqMap) {
    int generatedTasksCount = 0;
    try {
      ServiceJobTraceProducer stjp = new ServiceJobTraceProducer(rmConf);
      for (; generatedTasksCount < numServiceTasks;) {
        ServiceJobStory sjs = (ServiceJobStory) stjp.getNextJob();
        if (sjs == null) {
          throw new java.io.IOException(
              "Run out of Service jobs, consider increasing count of total jobs in 'experimentConf.json'");
        }
        ApplicationId currentApplicationId = sjs.generateApplicaitonId();
        appCMap.put(currentApplicationId, sjs.getConstraintDefinition());
        List<ResourceRequest> serviceRRs =
            sjs.generateFlattenedResourceRequests();
        for (ResourceRequest rr : serviceRRs) {
          // Ignore Mem std dev for now
          rr.getCapability().setMemorySize(
              (int) (rr.getCapability().getMemorySize() / 1024) * GB);
          appReqMap.putIfAbsent(currentApplicationId, new ArrayList<>());
          if (!appReqMap.get(currentApplicationId).contains(rr)
              && (generatedTasksCount < numServiceTasks)) {
            appReqMap.get(currentApplicationId).add(rr);
            generatedTasksCount++;
          }
        }
      }
    } catch (java.io.IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return generatedTasksCount;
  }

}
