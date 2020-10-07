package eu.fiwoo.rabbitmqtest;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ConsumerAutoACK {
	
	public static boolean AUTO_ACK = true;

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

		channel.basicConsume(
				Data.TOPIC, 
				AUTO_ACK, 
				(consumerTag, delivery) -> {

					// Delivery callback

					String message = new String(delivery.getBody(), "UTF-8");

					System.out.println(" [x] Received '" + message + "'");

					try {

						doWork(message);
						
					} catch (Exception e) {

						System.out.println(e);

					} finally {

						System.out.println(" [x] Done");
					}

				}, consumerTag -> {

					// Cancel callback

					System.out.println("Cancel callback");
				});
	}

	private static void doWork(String task) throws InterruptedException {

		Thread.sleep(2000);
	}
}
