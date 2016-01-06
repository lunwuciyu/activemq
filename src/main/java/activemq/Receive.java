package activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

public class Receive {

	private static String url = "tcp://localhost:61616";
	private static String topic = "alarm.msg.topic";
	private static String user = "";
	private static String password = "";
	private static Logger logger = Logger.getLogger(Receive.class);

	public void receiveMessage() {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, url);
		Connection connection;
		try {
			connection = connectionFactory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createTopic(topic);
			
			MessageConsumer consumer = session.createConsumer(destination);
			consumer.setMessageListener(new MessageListener() {
				public void onMessage(Message message) {
					TextMessage tm = (TextMessage) message;
					try {
						System.out.println("Received message: " + tm.getText());
					} catch (JMSException e) {
						logger.error(e.getMessage(),e);
					}
				}
			});
		} catch (JMSException e) {
			logger.error(e.getMessage(),e);
		}
	}

	public static void main(String[] args) {
		Receive receiveMessageFromMQ = new Receive();
		receiveMessageFromMQ.receiveMessage();
	}
}
