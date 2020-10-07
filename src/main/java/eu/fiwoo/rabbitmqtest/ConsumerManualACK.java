package eu.fiwoo.rabbitmqtest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

public class ConsumerManualACK {
	
	public static boolean AUTO_ACK = false;

	public static void main(String[] args) throws IOException, TimeoutException {

		System.out.println("Starting consumer...");

		ConnectionFactory factory = new ConnectionFactory();

		factory.setHost(Data.BROKER_HOST);
		factory.setPort(Data.BROKER_PORT);
		factory.setUsername(Data.BROKER_USER);
		factory.setPassword(Data.BROKER_PASS);

		Connection connection = factory.newConnection();

		Channel channel = connection.createChannel();

		channel.queueDeclare(
				Data.TOPIC, 
				true, 
				false, 
				false, 
				null);
		
		while (true ) {
					
			GetResponse response = channel.basicGet(Data.TOPIC, false);
			
			if (response != null) {
				
				Envelope env = response.getEnvelope();
				
				final long deliveryTag = env.getDeliveryTag();
				
				final String msg = new String(response.getBody(), StandardCharsets.UTF_8);
				
				System.out.println(" [x] Received '" + msg + "'");
				
				try {

					doWork(msg);
					
				} catch (Exception e) {

					System.out.println(e);

				} finally {

					System.out.println(" [x] Done");
				}
				
				channel.basicAck(deliveryTag, false);
			}
		}
	}

	private static void doWork(String task) throws InterruptedException {

		Thread.sleep(2000);
	}
}
