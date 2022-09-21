import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;







public class Controller implements Runnable {

    private static final Logger log = LogManager.getLogger(Controller.class);
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Long sleep;
    static double doublesleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static Instant lastScaleTime;
    static long joiningTime;

    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    //////////////////////////////////////////////////////////////////////////////
    static TopicDescription td;
    static DescribeTopicsResult tdr;
    static ArrayList<Partition> partitions = new ArrayList<>();

    static double dynamicTotalMaxConsumptionRate = 0.0;
    static double dynamicAverageMaxConsumptionRate = 0.0;

    static double wsla = 5.0;
    static List<Consumer> assignment;
    static Instant lastScaleUpDecision;
    static Instant lastScaleDownDecision;
    static Instant lastCGQuery;


    ///////////////////////////////////////////////
    static Map<Double, Integer> previousConsumers = new HashMap<>();

    static Map<Double, Integer> currentConsumers =  new HashMap<>();

    final static      List<Double> capacities = Arrays.asList(95.0, 180.0);

    public static List<Consumer> newassignment = new ArrayList<>();

    public static  Instant warmup = Instant.now();

    static ArrayList<Partition> topicpartitions = new ArrayList<>();


    //////////////////////////////////////////////////




    private static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {

        for (double c : capacities) {
            currentConsumers.put(c, 0);
            previousConsumers.put(c,0);
        }
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        tdr = admin.describeTopics(Collections.singletonList(topic));
        td = tdr.values().get(topic).get();
        lastScaleUpDecision = Instant.now();
        lastScaleDownDecision = Instant.now();
        lastCGQuery = Instant.now();

        for (TopicPartitionInfo p : td.partitions()) {
            partitions.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic has the following partitions {}", td.partitions().size());
        //previousConsumers.put(100.0, 1);

    }


    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(Controller.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();

        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

        dynamicTotalMaxConsumptionRate = 0.0;
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members()) {
            log.info("Calling the consumer {} for its consumption rate ", memberDescription.host());

            float rate = callForConsumptionRate(memberDescription.host());
            dynamicTotalMaxConsumptionRate += rate;
        }

        dynamicAverageMaxConsumptionRate = dynamicTotalMaxConsumptionRate /
                (float) consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();

        log.info("The total consumption rate of the CG is {}", String.format("%.2f",dynamicTotalMaxConsumptionRate));
        log.info("The average consumption rate of the CG is {}", String.format("%.2f", dynamicAverageMaxConsumptionRate));
    }


    private static float callForConsumptionRate(String host) {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(host.substring(1), 5002)
                .usePlaintext()
                .build();
        RateServiceGrpc.RateServiceBlockingStub rateServiceBlockingStub
                = RateServiceGrpc.newBlockingStub(managedChannel);
        RateRequest rateRequest = RateRequest.newBuilder().setRate("Give me your rate")
                .build();
        log.info("connected to server {}", host);
        RateResponse rateResponse = rateServiceBlockingStub.consumptionRate(rateRequest);
        log.info("Received response on the rate: " + String.format("%.2f",rateResponse.getRate()));
        managedChannel.shutdown();
        return rateResponse.getRate();
    }

    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets2 = new HashMap<>();

        for (TopicPartitionInfo p : td.partitions()) {
            requestLatestOffsets.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
            requestTimestampOffsets2.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1500).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(sleep + 1500).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                admin.listOffsets(requestTimestampOffsets2).all().get();


        long totalArrivalRate = 0;
        double currentPartitionArrivalRate;
        Map<Integer, Double> previousPartitionArrivalRate = new HashMap<>();
        for (TopicPartitionInfo p : td.partitions()) {
            previousPartitionArrivalRate.put(p.partition(), 0.0);
        }
        for (TopicPartitionInfo p : td.partitions()) {
            TopicPartition t = new TopicPartition(topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long timeoffset1 = timestampOffsets1.get(t).offset();
            long timeoffset2 = timestampOffsets2.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();
            partitions.get(p.partition()).setLag(latestOffset - committedoffset);
            //TODO if abs(currentPartitionArrivalRate -  previousPartitionArrivalRate) > 15
            //TODO currentPartitionArrivalRate= previousPartitionArrivalRate;
            if(timeoffset2==timeoffset1)
                break;

            if (timeoffset2 == -1) {
                timeoffset2 = latestOffset;
            }
            if (timeoffset1 == -1) {
                // NOT very critical condition
                currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                log.info("Arrival rate into partition {} is {}", t.partition(), partitions.get(p.partition()).getArrivalRate());
                log.info("lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));
            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / doublesleep;
                //if(currentPartitionArrivalRate==0) continue;
                //TODO only update currentPartitionArrivalRate if (currentPartitionArrivalRate - previousPartitionArrivalRate) < 10 or threshold
                // currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                //TODO break
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                log.info(" Arrival rate into partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getArrivalRate());

                log.info(" lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));
            }
            //TODO add a condition for when both offsets timeoffset2 and timeoffset1 do not exist, i.e., are -1,
            previousPartitionArrivalRate.put(p.partition(), currentPartitionArrivalRate);
            totalArrivalRate += currentPartitionArrivalRate;
        }
        //report total arrival only if not zero only the loop has not exited.
        log.info("totalArrivalRate {}", totalArrivalRate);


        // attention not to have this CG querying interval less than cooldown interval
      /*  if (Duration.between(lastCGQuery, Instant.now()).toSeconds() >= 30) {
            queryConsumerGroup();
            lastCGQuery = Instant.now();
        }*/
        //youMightWanttoScaleUsingBinPack();
        youMightWanttoScaleTrial2();
    }


    public static void  youMightWanttoScaleTrial2(){

        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions);
        dynamicAverageMaxConsumptionRate = 95.0;
        Map<Double, Consumer> currentConsumersByName = new HashMap<>();
        LeastLoadedFFD llffd = new LeastLoadedFFD(parts, 95.0);
        List<Consumer> cons = llffd.LeastLoadFFDHeterogenous();

        log.info("we currently need this consumer");
        log.info(cons);

        newassignment.clear();

        newassignment.addAll(cons);


        for (Consumer co: cons) {
            currentConsumers.put(co.getCapacity(), currentConsumers.getOrDefault(co.getCapacity() +1 ,1));
            currentConsumersByName.put(co.getCapacity(), co);
        }




        for (double d : currentConsumers.keySet()) {
            log.info("current consumer capacity {}, {}", d, currentConsumers.get(d));

        }


        Map<Double, Integer> scaleByCapacity = new HashMap<>();

        Map<Double, Integer>  diffByCapacity = new HashMap<>();


        for (double d : currentConsumers.keySet()) {
            if (currentConsumers.get(d).equals(previousConsumers.get(d))) {
                log.info("No need to scale consumer of capacity {}", d);
            }
            int factor2 = currentConsumers.get(d);

            for (int i =0;i<factor2; i++) {
                currentConsumersByName.get(d).setId("cons"+(int)d+ "-" + i);
            }

            int factor = currentConsumers.get(d); /*- previousConsumers.get(d);*/
            int  diff = currentConsumers.get(d) - previousConsumers.get(d);
            log.info("diff {} for capacity {}", diff, d);
            diffByCapacity.put(d, diff);

            scaleByCapacity.put(d, factor);
            log.info(" the consumer of capacity {} shall be scaled to {}", d, factor);
        }






        log.info("current consumers");


        for (double d : capacities) {

/*            if(diffByCapacity.get(d) == -1)
                continue;*/

            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d)!=0) {
                log.info("The statefulset {} shalll be  scaled to {}", "cons"+(int)d, scaleByCapacity.get(d) );
                if(Duration.between(warmup, Instant.now()).toSeconds() > 30 ) {
                    log.info("cons"+(int)d);

                    new Thread(()-> { try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons"+(int)d).scale(scaleByCapacity.get(d));
                    }}).start();

                }


            }

        }



        for (double d : capacities) {
            log.info(currentConsumersByName.get(d));

            previousConsumers.put(d, currentConsumers.get(d));
            currentConsumers.put(d, 0);
        }





      /*  try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
            k8s.apps().statefulSets().inNamespace("default").withName()
                    deployments().inNamespace("default").withName("cons1persec").scale(reco);
        }*/





    }




   /* private static void youMightWanttoScaleUsingBinPack() {
        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        if(Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() >= 30) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }*/


  /*  public static void scaleAsPerBinPack(int currentsize) {

        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          *//*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*//*
        } else if (replicasForscale > 0) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Upscaled you should have {}", neededsize);
            }

            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
            lastScaleTime = Instant.now();
        } else {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Downscaled you should have {}", neededsize);
                lastScaleUpDecision = Instant.now();
                lastScaleDownDecision = Instant.now();
                lastCGQuery = Instant.now();
                lastScaleTime = Instant.now();
            }
        }
    }
*/

   /* private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;

        List<Partition> parts = new ArrayList<>(partitions);
        dynamicAverageMaxConsumptionRate = 95.0;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);
        consumers.add(new Consumer(consumerCount, maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }

        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {
                //TODO externalize these choices on the inout to the FFD bin pack
                // TODO  hey stupid use instatenous lag instead of average lag.
                // TODO average lag is a decision on past values especially for long DI.
                if (cons.getRemainingLagCapacity() >=  partition.getLag() *//*partition.getAverageLag()*//* &&
                        cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer(consumerCount, (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }

        log.info(" The BP scaler recommended {}", consumers.size());

        // copy consumers and partitions for fair assignment
        List<Consumer> fairconsumers = new ArrayList<>(consumers.size());
        List<Partition> fairpartitions= new ArrayList<>();

        for (Consumer cons : consumers) {
            fairconsumers.add(new Consumer(cons.getId(), maxLagCapacity, dynamicAverageMaxConsumptionRate));
            for(Partition p : cons.getAssignedPartitions()){
                fairpartitions.add(p);
            }
        }

        //sort partitions in descending order for debugging purposes
        fairpartitions.sort(new Comparator<Partition>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                return Double.compare(o2.getArrivalRate(), o1.getArrivalRate());
            }
        });

        //1. list of consumers that will contain the fair assignment
        //2. list of consumers out of the bin pack.
        //3. the partition sorted in their decreasing arrival rate.
        assignPartitionsFairly(fairconsumers,consumers,fairpartitions);





        for (Consumer cons : fairconsumers) {
            log.info("fair consumer {} is assigned the following partitions", cons.getId() );
            for(Partition p : cons.getAssignedPartitions()) {
                log.info("fair Partition {}", p.getId());
            }
        }

        assignment = fairconsumers;
        return consumers.size();
    }



    public static void assignPartitionsFairly(
            final List<Consumer> assignment,
            final List<Consumer> consumers,
            final List<Partition> partitionsArrivalRate) {
        if (consumers.isEmpty()) {
            return;
        }// Track total lag assigned to each consumer (for the current topic)
        final Map<Integer, Double> consumerTotalArrivalRate = new HashMap<>(consumers.size());
        final Map<Integer, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        final Map<Integer, Double> consumerRemainingAllowableArrivalRate = new HashMap<>(consumers.size());
        final Map<Integer, Double> consumerAllowableArrivalRate = new HashMap<>(consumers.size());


        for (Consumer cons : consumers) {
            consumerTotalArrivalRate.put(cons.getId(), 0.0);
            consumerAllowableArrivalRate.put(cons.getId(), 95.0);
        }

        // Track total number of partitions assigned to each consumer (for the current topic)
        for (Consumer cons : consumers) {
            consumerTotalPartitions.put(cons.getId(), 0);
            consumerRemainingAllowableArrivalRate.put(cons.getId(), consumerAllowableArrivalRate.get(cons.getId()));
        }

        // might want to remove, the partitions are sorted anyway.
        //First fit decreasing
        partitionsArrivalRate.sort((p1, p2) -> {
            // If lag is equal, lowest partition id first
            if (p1.getArrivalRate() == p2.getArrivalRate()) {
                return Integer.compare(p1.getId(), p2.getId());
            }
            // Highest arrival rate first
            return Double.compare(p2.getArrivalRate(), p1.getArrivalRate());
        });
        for (Partition partition : partitionsArrivalRate) {
            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id arrival rate
            // returns the consumer with lowest assigned partitions, if all assigned partitions equal returns the min total arrival rate
            final Integer memberId = Collections
                    .min(consumerTotalArrivalRate.entrySet(), (c1, c2) -> {
                        // Lowest partition count first

                        //TODO is that necessary partition count first... not really......
                        //lowest number of partitions first.
                        final int comparePartitionCount = Integer.compare(consumerTotalPartitions.get(c1.getKey()),
                                consumerTotalPartitions.get(c2.getKey()));
                        if (comparePartitionCount != 0) {
                            return comparePartitionCount;
                        }
                        // If partition count is equal, lowest total lag first, get the consumer with the lowest arrival rate
                        final int compareTotalLags = Double.compare(c1.getValue(), c2.getValue());
                        if (compareTotalLags != 0) {
                            return compareTotalLags;
                        }
                        // If total arrival rate  is equal, lowest consumer id first
                        return c1.getKey().compareTo(c2.getKey());
                    }).getKey();
            //we currently have the the consumer with the lowest lag
*//*            System.out.println("Assigning the consumer {} with the lowest ArrivalRate {} to the partition with the highest ArrivalRate {} "+
                    memberId + "  " + consumerTotalArrivalRate.get(memberId) + " " + partition.getArrivalRate());*//*



            assignment.get(memberId).assignPartition(partition);
            consumerTotalArrivalRate.put(memberId, consumerTotalArrivalRate.getOrDefault(memberId, 0.0) + partition.getArrivalRate());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);
            consumerRemainingAllowableArrivalRate.put(memberId, consumerAllowableArrivalRate.get(memberId)
                    - consumerTotalArrivalRate.get(memberId));
           *//* System.out.println("The remaining allowable arrival rate for consumer {} is {} " +
                    memberId + " " + (consumerAllowableArrivalRate.get(memberId) - consumerTotalArrivalRate.get(memberId)));*//*

//            System.out.println(
//                    "Assigned partition {}-{} to consumer {}.  partition_arrival_rate={}, consumer_current_total_arrival_rate{} " +
//                            " " + partition.getId() +
//                            " " + memberId +
//                            " " + partition.getArrivalRate() +
//                            " " + consumerTotalArrivalRate.get(memberId));

            log.info(
                    "Assigned partition {} to consumer {}.  partition_arrival_rate={}, consumer_current_total_arrival_rate{} ",
                    partition.getId(),
                    memberId ,
                    String.format("%.2f", partition.getArrivalRate()) ,
                    consumerTotalArrivalRate.get(memberId));
        }
    }*/


    @Override
    public void run() {
        try {
            readEnvAndCrateAdminClient();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        doublesleep = (double) sleep / 1000.0;
        try {
            //Initial delay so that the producer has started.
            Thread.sleep(30*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            log.info("New Iteration:");
            try {
                getCommittedLatestOffsetsAndLag();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Sleeping for {} seconds", sleep / 1000.0);
            log.info("End Iteration;");
            log.info("============================================");
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}



