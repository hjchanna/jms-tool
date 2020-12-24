package me.channa.jmstool.commands;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.log4j.Logger;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Enumeration;
import java.util.Properties;

@Parameters(commandDescription = "Consume messages from a JMS queue")
public class ConsumeCommand implements ICommand {
    private static final Logger LOGGER = Logger.getLogger(ConsumeCommand.class);

    @Parameter(names = "-url", required = true, description = "AMQP connection url of the message broker (amqp://[<user>:<pass>@][<clientid>]<virtualhost>[?brokerlist='<broker url>[;<broker url>]'])")
    private String url;

    @Parameter(names = "-queue", required = true, description = "JMS message queue name to consume messages")
    private String queue;

    @Parameter(names = "-count", description = "Number of messages to be consumed")
    private int count = 1;

    @Parameter(names = "-timeout", description = "Message receive timeout in milli-seconds")
    private long timeout = 5000;

    @Parameter(names = "-nowait", description = "Don't wait for messages")
    private boolean nowait = false;

    private static final String QPID_INITIAL_CONNECTION_FACTORY = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CONNECTION_FACTORY_NAME_PREFIX = "connectionfactory.";
    private static final String CONNECTION_FACTORY_NAME = "qpidConnectionfactory";

    @Override
    public void execute(JCommander jCommander) {
        LOGGER.debug(String.format("JMS consume command received with params - url: %s; queue: %s; count: %d; timeout: %d; nowait: %s", url, queue, count, timeout, nowait));

        QueueConnection queueConnection = null;
        QueueSession queueSession = null;
        MessageConsumer consumer = null;

        try {
            // JNDI properties
            Properties properties = new Properties();
            properties.put(Context.INITIAL_CONTEXT_FACTORY, this.QPID_INITIAL_CONNECTION_FACTORY);
            properties.put(this.CONNECTION_FACTORY_NAME_PREFIX + this.CONNECTION_FACTORY_NAME, this.url);
            properties.put("queue." + this.queue, this.queue);
            LOGGER.debug(String.format("JNDI properties initiated: %s", properties));

            InitialContext ctx = new InitialContext(properties);
            LOGGER.debug("InitialContext created for supplied properties");

            // Lookup connection factory
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(this.CONNECTION_FACTORY_NAME);
            queueConnection = connFactory.createQueueConnection();
            LOGGER.debug("Queue connection established");

            // JMS connection and the session
            queueConnection.start();
            queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
            LOGGER.debug("Queue session initiated");

            // Queue consumer
            Queue queue = (Queue) ctx.lookup(this.queue);
            consumer = queueSession.createConsumer(queue);
            LOGGER.debug("Queue consumer initiated");

            // Iterate over the number of message count to receive
            for (int i = 0; i < count; i++) {
                // receive the message within the timeout
                Message message;
                if (nowait) {
                    message = consumer.receiveNoWait();
                } else {
                    message = consumer.receive(timeout);
                }

                // consume the message and log message details
                if (message != null) {
                    StringBuilder stringBuilder = new StringBuilder("New message consumed:\n");

                    Enumeration messagePropertyNames = message.getPropertyNames();
                    while (messagePropertyNames.hasMoreElements()) {
                        String propertyName = messagePropertyNames.nextElement().toString();
                        stringBuilder
                                .append("[JMS-PROPERTY] ")
                                .append(propertyName)
                                .append(":")
                                .append(message.getStringProperty(propertyName))
                                .append("\n");
                    }

                    stringBuilder.append("[JMS-MESSAGE]: ")
                            .append(((TextMessage) message).getText());

                    LOGGER.info(stringBuilder.toString());
                }

                //TODO: persist message for further reference:
                //            Object messageData;
                //            if (message instanceof TextMessage) {
                //                messageData = ((TextMessage) message).getText();
                //            } else if (message instanceof ObjectMessage) {
                //                BytesMessage message1 = (BytesMessage) message;
                //                byte[] bytes = new byte[(int) message1.getBodyLength()];
                //                message1.readBytes(bytes);
                //                messageData = bytes;
                //            } else if (message instanceof BytesMessage) {
                //                messageData = ((ObjectMessage) message).getObject();
                //            } else if (message instanceof StreamMessage) {
                //                StreamMessage streamMessage = (StreamMessage) message;
                //                messageData = streamMessage.readObject();
                //            } else {
                //                messageData = new byte[]{};
                //            }
            }

        } catch (NamingException | JMSException exception) {
            LOGGER.error("An error occurred while consuming messages", exception);
        } finally {
            LOGGER.debug("Closing active connections if available");
            try {
                if (consumer != null) {
                    consumer.close();
                }

                if (queueConnection != null) {
                    queueSession.close();
                }

                if (queueConnection != null) {
                    queueConnection.stop();
                    queueConnection.close();
                }
            } catch (Exception exception) {
                LOGGER.error("Error occurred while closing JMS connections", exception);
            }
        }
    }
}
