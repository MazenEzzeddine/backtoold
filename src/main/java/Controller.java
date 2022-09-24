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

    final static      List<Double> capacities = Arrays.asList(95.0, 240.0);

    public static List<Consumer> newassignment = new ArrayList<>();

    public static  Instant warmup = Instant.now();

    static ArrayList<Partition> topicpartitions = new ArrayList<>();


    //////////////////////////////////////////////////

    static Instant lastScaletime;

    static boolean scaled = false;




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


    static double  previousTotalArrivalRate = 0.0;

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
             /*   log.info("Arrival rate into partition {} is {}", t.partition(), partitions.get(p.partition()).getArrivalRate());
                log.info("lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));*/
            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / doublesleep;
                //if(currentPartitionArrivalRate==0) continue;
                //TODO only update currentPartitionArrivalRate if (currentPartitionArrivalRate - previousPartitionArrivalRate) < 10 or threshold
                // currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                //TODO break
              partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                 /* log.info(" Arrival rate into partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getArrivalRate());

                log.info(" lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));*/
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

    /*    if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 30)
        youMightWanttoScaleTrial2();*/

      /*  if(Math.abs(totalArrivalRate-previousTotalArrivalRate) > 10) {
            return;
        }

        previousTotalArrivalRate = totalArrivalRate;
*/


        if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 30)
            youMightWanttoScaleTrial2();
    }


    public static void  youMightWanttoScaleTrial2(){

        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions);
        Map<Double, List<Consumer>> currentConsumersByName = new HashMap<>();
    /*   LeastLoadedFFD llffd = new LeastLoadedFFD(parts, 95.0);
        List<Consumer> cons = llffd.LeastLoadFFDHeterogenous();*/

        FirstFitDecHetero hetero = new FirstFitDecHetero(parts, capacities);
        List<Consumer> cons = hetero.fftFFDHetero();


        for(double c : capacities) {
            currentConsumersByName.putIfAbsent(c, new ArrayList<>());

        }


        log.info("we currently need this consumer");
        log.info(cons);
        newassignment.clear();

        for (Consumer co: cons) {
            log.info(co.getCapacity());
            currentConsumers.put(co.getCapacity(), currentConsumers.get(co.getCapacity()) +1);
            currentConsumersByName.get(co.getCapacity()).add(co);
           // currentConsumersByName.put(co.getCapacity(), co);
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


            int index=0;
            for (Consumer c:  currentConsumersByName.get(d)) {
                c.setId("cons"+(int)d+ "-" + index);
                index++;
                log.info(c.getId());
            }



            int factor = currentConsumers.get(d); /*- previousConsumers.get(d);*/
            int  diff = currentConsumers.get(d) - previousConsumers.get(d);
            log.info("diff {} for capacity {}", diff, d);
            diffByCapacity.put(d, diff);

            scaleByCapacity.put(d, factor);
            log.info(" the consumer of capacity {} shall be scaled to {}", d, factor);
        }

        newassignment.addAll(cons);







        for (double d : capacities) {
            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d)!=0) {
                log.info("The statefulset {} shall be  scaled to {}", "cons"+(int)d, scaleByCapacity.get(d) );
                if(Duration.between(warmup, Instant.now()).toSeconds() > 30 ) {
                    log.info("cons"+(int)d);

                    new Thread(()-> { try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons"+(int)d).scale(scaleByCapacity.get(d));
                    }}).start();
                    lastScaletime = Instant.now();
                }
            }
        }

        for (double d : capacities) {

            previousConsumers.put(d, currentConsumers.get(d));
            currentConsumers.put(d, 0);
        }

    }







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
            lastScaletime = Instant.now();
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



