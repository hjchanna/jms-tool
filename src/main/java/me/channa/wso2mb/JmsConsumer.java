package me.channa.wso2mb;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class JmsConsumer {

    @Parameter(names = "-help", help = true)
    private boolean help = false;

    @Parameter(names = "-user")
    String userName = "admin";

    @Parameter(names = "-password")
    String password = "admin";

    @Parameter(names = "-clientid")
    private static String CARBON_CLIENT_ID = "carbon";

    @Parameter(names = "-virtualhost")
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";

    @Parameter(names = "-hostname")
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";

    @Parameter(names = "-port")
    private static String CARBON_DEFAULT_PORT = "5672";

    @Parameter(names = "-queue")
    String queueName;


    private static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";


    private QueueConnection queueConnection;
    private QueueSession queueSession;

    public static void main(String[] args) throws NamingException, JMSException {
        //initiate
        JmsConsumer consumer = new JmsConsumer();
        JCommander jCommander = JCommander.newBuilder()
                .addObject(consumer)
                .build();

        //help
        if (consumer.help) {
            jCommander.usage();
            System.exit(0);
            return;
        }

        //parse args
        jCommander.parse(args);

        //validate
        if (consumer.queueName == null) {
            System.err.println("Please enter valid parameters");
            jCommander.usage();
            System.exit(1);
        }

        //consume jms
        MessageConsumer messageConsumer = consumer.registerSubscriber();
        consumer.receiveMessages(messageConsumer);
    }

    public MessageConsumer registerSubscriber() throws NamingException, JMSException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        queueSession =
                queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        //Receive message
        Queue queue = (Queue) ctx.lookup(queueName);
        MessageConsumer consumer = queueSession.createConsumer(queue);
        return consumer;
    }

    public void receiveMessages(MessageConsumer consumer) throws NamingException, JMSException {
        TextMessage message = (TextMessage) consumer.receive();
        System.out.println("Got message from queue receiver==>" + message.getText());
        // Housekeeping
        consumer.close();
        queueSession.close();
        queueConnection.stop();
        queueConnection.close();
    }

    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }
}
