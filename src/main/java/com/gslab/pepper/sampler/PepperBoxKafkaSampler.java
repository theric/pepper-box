
package com.gslab.pepper.sampler;

import com.eclipsesource.json.*;
import com.gslab.pepper.util.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.*;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.*;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.zookeeper.*;
import org.slf4j.*;

/**
 * The PepperBoxKafkaSampler class custom java sampler for jmeter.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class PepperBoxKafkaSampler extends AbstractJavaSamplerClient {

    //kafka producer
    private KafkaProducer<String, Object> producer;

    // topic on which messages will be sent
    private String topic;

    //Message placeholder key
    private String placeHolder;

    private static final Logger log = LoggerFactory.getLogger(PepperBoxKafkaSampler.class);


    /**
     * Set default parameters and their values
     *
     * @return
     */
    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeys.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, ProducerKeys.KAFKA_TOPIC_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerKeys.KEY_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerKeys.VALUE_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerKeys.COMPRESSION_TYPE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BATCH_SIZE_CONFIG, ProducerKeys.BATCH_SIZE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.LINGER_MS_CONFIG, ProducerKeys.LINGER_MS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BUFFER_MEMORY_CONFIG, ProducerKeys.BUFFER_MEMORY_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.ACKS_CONFIG, ProducerKeys.ACKS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.SEND_BUFFER_CONFIG, ProducerKeys.SEND_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.RECEIVE_BUFFER_CONFIG, ProducerKeys.RECEIVE_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        defaultParameters.addArgument(PropsKeys.MESSAGE_PLACEHOLDER_KEY, PropsKeys.MSG_PLACEHOLDER);
        defaultParameters.addArgument(ProducerKeys.KERBEROS_ENABLED, ProducerKeys.FLAG_NO);
        defaultParameters.addArgument(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.JAVA_SEC_KRB5_CONFIG, ProducerKeys.JAVA_SEC_KRB5_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, ProducerKeys.SASL_KERBEROS_SERVICE_NAME_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_MECHANISM, ProducerKeys.SASL_MECHANISM_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_JAAS_CONFIG, ProducerKeys.SASL_JAAS_CONFIG_DEFAULT);

        defaultParameters.addArgument(ProducerKeys.SSL_ENABLED, ProducerKeys.FLAG_NO);
        defaultParameters.addArgument(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "<Key Password>");
        defaultParameters.addArgument(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "<Keystore Location>");
        defaultParameters.addArgument(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "<Keystore Password>");
        defaultParameters.addArgument(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "<Truststore Location>");
        defaultParameters.addArgument(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "<Truststore Password>");



        return defaultParameters;
    }

    /**
     * Gets invoked exactly once  before thread starts
     *
     * @param context
     */
    @Override
    public void setupTest(JavaSamplerContext context) {
        try {
            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerServers(context));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            props.put(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
            props.put(ProducerConfig.SEND_BUFFER_CONFIG, context.getParameter(ProducerConfig.SEND_BUFFER_CONFIG));
            props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, context.getParameter(ProducerConfig.RECEIVE_BUFFER_CONFIG));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
            props.put(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, context.getParameter(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            props.put(ProducerKeys.SASL_MECHANISM, context.getParameter(ProducerKeys.SASL_MECHANISM));
            props.put(ProducerKeys.SASL_JAAS_CONFIG, context.getParameter(ProducerKeys.SASL_JAAS_CONFIG));

            Iterator<String> parameters = context.getParameterNamesIterator();
            parameters.forEachRemaining(parameter -> {
                if (parameter.startsWith("_")) {
                    props.put(parameter.substring(1), context.getParameter(parameter));
                }
            });


            String sslEnabled = context.getParameter(ProducerKeys.SSL_ENABLED);

            if (sslEnabled != null && sslEnabled.equals(ProducerKeys.FLAG_YES)) {

                props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, context.getParameter(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, context.getParameter(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            }

            String kerberosEnabled = context.getParameter(ProducerKeys.KERBEROS_ENABLED);
            if (kerberosEnabled != null && kerberosEnabled.equals(ProducerKeys.FLAG_YES)) {
                System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
                System.setProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_KRB5_CONFIG));
                props.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, context.getParameter(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
            }

            placeHolder = context.getParameter(PropsKeys.MESSAGE_PLACEHOLDER_KEY);
            topic = context.getParameter(ProducerKeys.KAFKA_TOPIC_CONFIG);
            producer = new KafkaProducer<String, Object>(props);
        }catch (Exception e){
            log.error("producer init failed");
        }



    }


    /**
     * For each sample request this method is invoked and will return success/failure result
     *
     * @param context
     * @return
     */
    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        try {
            String message = genMessageVariables();
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(topic, message);
            producer.send(producerRecord).get();
            sampleResult.setResponseData(message, StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(true);
            sampleResult.sampleEnd();

        } catch (Exception e) {
            String errorMessage = null != e && null != e.getMessage() ? e.getMessage():"failed to send message";
            sampleResult.setResponseData(errorMessage, StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(false);
            sampleResult.sampleEnd();
        }
        return sampleResult;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if(producer != null){
            log.info("producer close");
            producer.close();
        }

    }

    private String getBrokerServers(JavaSamplerContext context) {

        StringBuilder kafkaBrokers = new StringBuilder();

        String zookeeperServers = context.getParameter(ProducerKeys.ZOOKEEPER_SERVERS);

        if (zookeeperServers != null && !zookeeperServers.equalsIgnoreCase(ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT)) {

            try {

                ZooKeeper zk = new ZooKeeper(zookeeperServers, 10000, null);
                List<String> ids = zk.getChildren(PropsKeys.BROKER_IDS_ZK_PATH, false);

                for (String id : ids) {

                    String brokerInfo = new String(zk.getData(PropsKeys.BROKER_IDS_ZK_PATH + "/" + id, false, null));
                    JsonObject jsonObject = Json.parse(brokerInfo).asObject();

                    String brokerHost = jsonObject.getString(PropsKeys.HOST, "");
                    int brokerPort = jsonObject.getInt(PropsKeys.PORT, -1);

                    if (!brokerHost.isEmpty() && brokerPort != -1) {

                        kafkaBrokers.append(brokerHost);
                        kafkaBrokers.append(":");
                        kafkaBrokers.append(brokerPort);
                        kafkaBrokers.append(",");

                    }

                }
            } catch (IOException | KeeperException | InterruptedException e) {

                log.error("Failed to get broker information", e);

            }

        }

        if (kafkaBrokers.length() > 0) {

            kafkaBrokers.setLength(kafkaBrokers.length() - 1);

            return kafkaBrokers.toString();

        } else {

            return  context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        }
    }

    public String genMessageVariables(){
        JMeterVariables variables = JMeterContextService.getContext().getVariables();
        String message = variables.getObject(placeHolder).toString();
        for (Entry<String, Object> var:variables.entrySet()){
            String key = var.getKey();
            String value = var.getValue().toString();
            String definedVariable = "${" + key + "}";
            if(message.contains(definedVariable)){
                message = message.replace(definedVariable,value);
            }
        }
        return message;
    }
}
