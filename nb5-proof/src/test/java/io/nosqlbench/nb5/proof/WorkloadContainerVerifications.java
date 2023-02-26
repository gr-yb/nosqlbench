/*
 * Copyright (c) 2023 nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.nb5.proof;




import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.nosqlbench.engine.api.scenarios.NBCLIScenarioParser;
import io.nosqlbench.engine.api.scenarios.WorkloadDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class WorkloadContainerVerifications {
    private enum Driver {
        CQL("cql"),
        HTTP("http"),
        MONGODB("mongodb"),
        TCP ("tcp"),
        PULSAR("pulsar"),
        DYNAMODB("dynamo"),
        KAFKA("kafka");
        private final String name;
        Driver(String name) {
            this.name = name;
        }
        public String getName() {
            return name;
        }
    }
    public static Logger logger = LogManager.getLogger(WorkloadContainerVerifications.class);
    private final String java = Optional.ofNullable(System.getenv(
        "JAVA_HOME")).map(v -> v + "/bin/java").orElse("java");
    private final static String JARNAME = "../nb5/target/nb5.jar";
    private final static String BASIC_CHECK_IDENTIFIER = "basic_check";
    private static final CassandraContainer cass = (CassandraContainer) new CassandraContainer(DockerImageName.parse("cassandra:latest"))
        .withExposedPorts(9042).withAccessToHost(true);
    private static Map<Driver, List<WorkloadDesc>> basicWorkloadsMapPerDriver = null;
    @BeforeAll
    public static void listWorkloads() {

        List<WorkloadDesc> workloads = List.of();
        basicWorkloadsMapPerDriver = new HashMap<>();
        try {
            workloads = NBCLIScenarioParser.getWorkloadsWithScenarioScripts(true, "examples");
        } catch (Exception e) {
            throw new RuntimeException("Error while getting workloads:" + e.getMessage(), e);
        }
        for (Driver driver : Driver.values()) {
            basicWorkloadsMapPerDriver.put(driver, getBasicCheckWorkloadsForDriver(workloads, BASIC_CHECK_IDENTIFIER, driver.getName()));
        }
    }


    @Test
    public void testCqlKeyValueWorkload() {
        ProcessInvoker invoker = new ProcessInvoker();
        invoker.setLogDir("logs/test");

        if(basicWorkloadsMapPerDriver.get(Driver.CQL) == null)
            return ;
        else if (basicWorkloadsMapPerDriver.get(Driver.CQL).size() == 0)
            return;

        for(WorkloadDesc workloadDesc : basicWorkloadsMapPerDriver.get(Driver.CQL))
        {
            String path = workloadDesc.getYamlPath();
            int lastSlashIndex = path.lastIndexOf('/');
            String shortName = path.substring(lastSlashIndex + 1);
            if(shortName.equals("cql-iot-dse.yaml"))
                continue;
            //STEP0:Start the test container and expose the 9042 port on the local host.
            //So that the docker bridge controller exposes the port to our process invoker that will run nb5
            //and target cassandra on that docker container
            cass.start();
            //the default datacenter name
            String datacenter = cass.getLocalDatacenter();
            //When running with a local Docker daemon, exposed ports will usually be reachable on localhost.
            // However, in some CI environments they may instead be reachable on a different host.
            //the port mapped to the original exposed port of the cassandra image
            Integer mappedPort9042 = cass.getMappedPort(9042);
            //the host ip of the cassandra image in the container
            String hostIP = cass.getHost();


           //STEP1: Run the example cassandra workload using the schema tag to create the Cass Baselines keyspace
            String[] args = new String[]{
                "java", "-jar", JARNAME, shortName, BASIC_CHECK_IDENTIFIER, "host="+ hostIP, "localdc="+ datacenter, "port="+ mappedPort9042.toString(), "table=keyvalue", "keyspace=baselines"
            };
            logger.info("The final command line: " + String.join(" ", args));
            ProcessResult runSchemaResult = invoker.run("run-workload", 30, args);


            //STEP 2 Check runSchemaOut for errors
            logger.info("Checking if the NB5 command resulted in any errors...");
            assertThat(runSchemaResult.exception).isNull();
            String runSchemaOut = String.join("\n", runSchemaResult.getStdoutData());
            assertThat(runSchemaOut.toLowerCase()).doesNotContain("error");
            logger.info("NB5 command completed with no errors");


            //STEP 3 Check the cluster for created data
            //THIS PART IS LEFT COMMENTED FOR RUNNING SPECIFIC CQL DATA CREATION CHECKING

//            try (CqlSession session = CqlSession.builder().addContactPoint(new InetSocketAddress(hostIP, mappedPort9042)).withLocalDatacenter(datacenter).build()) {
//                //->Check for the creation of the keyspace baselines
//                logger.info("Checking for the creation of the keyspace \"baselines\"...");
//                ResultSet result = session.execute("SELECT keyspace_name FROM system_schema.keyspaces");
//                List<Row> rows = result.all();
//                boolean keyspaceFound = false;
//                for (Row row : rows) {
//                    if (row.getString("keyspace_name").equals("baselines")) {
//                        keyspaceFound = true;
//                        break;
//                    }
//                }
//                assertTrue(keyspaceFound);
//                logger.info("Keyspace \"baselines\" was found, nb5 command had created it successfully");
//
//                //->Check for the creation of the baselines keyvalue table
//                logger.info("Checking for the creation of the table \"baselines.keyvalue\"...");
//                result = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='baselines'");
//                rows = result.all();
//                boolean tableFound = false;
//                for (Row row : rows) {
//                    if (row.getString("table_name").equals("keyvalue")) {
//                        tableFound = true;
//                        break;
//                    }
//                }
//                assertTrue(tableFound);
//                logger.info("Table \"baselines.keyvalue\" was found, nb5 command had created it successfully");
//
//                //->Check for the creation of the baselines keyvalue table
//                ResultSet resultSet = session.execute("SELECT count(*) FROM baselines.keyvalue");
//                Row row = resultSet.one();
//                long rowCount = row.getLong(0);
//                logger.info("Number of rows in baselines.keyvalue: " + rowCount);
//                assertTrue(rowCount >= 5);
//                logger.info("Table \"baselines.keyvalue\" has at least 5 rows of key-value pairs, nb5 command had created them successfully");
//
//            } catch (Exception e)
//            {
//                System.out.println(e.getMessage());
//                fail();
//            } finally {
                cass.stop();
//            }
        }
    }
    @AfterEach
    public void cleanup(){
        System.out.println("setup");
    }

    /*
    This method filters the input list of workloads to output the subset of workloads
    that include a specific scenario (input) and run the specified driver
    */
    public static List<WorkloadDesc> getBasicCheckWorkloadsForDriver(List<WorkloadDesc> workloads ,String scenarioFilter,  String driver) {
        String substring = "driver=" + driver;
        ArrayList<WorkloadDesc> workloadsForDriver = new ArrayList<>();
        for (WorkloadDesc workload : workloads) {
            if(workload.getScenarioNames().contains(scenarioFilter)) {
                try {
                    Path yamlPath = Path.of(workload.getYamlPath());
                    List<String> lines = Files.readAllLines(yamlPath);
                    for (String line : lines) {
                        if (line.contains(substring)) {
                            workloadsForDriver.add(workload);
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error reading file " + workload.getYamlPath() + ": " + e.getMessage());
                    break;
                }
            }
        }
        return workloadsForDriver;
    }


}
