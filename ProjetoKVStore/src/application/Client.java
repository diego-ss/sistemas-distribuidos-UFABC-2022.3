package application;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

public class Client {

	// relação de portas dos servidores
	private Set<Integer> serversPorts = new HashSet<Integer>();

	private static Scanner keyboard;

	public Client() {

	}

	public void addServerPort(Integer port) {
		serversPorts.add(port);
	}

	public void sendPutMessage(String key, String value) {
		Thread th = new Thread(() -> {

			// criando mensagem
			Message message = new Message();
			message.setAsPut("key", "value");
			
			Message response = sendMessage(message);

		});

		th.start();
	}

	public Message sendMessage(Message message) {
		try {
			// pegando servidor aleatório
			Integer port = getRandomPort();

			// criando socket e streams de escrita/leitura
			Socket s = new Socket("127.0.0.1", port);
			OutputStream os = s.getOutputStream();
			DataOutputStream writer = new DataOutputStream(os);
			InputStreamReader is = new InputStreamReader(s.getInputStream());
			BufferedReader reader = new BufferedReader(is);

			// enviando mensagem
			String msgJson = message.toJson();
			writer.writeBytes(msgJson + "\n");
			String response = reader.readLine();
			Message responseMsg = Message.fromJson(response);
			System.out.println(response);
			s.close();
			return responseMsg;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} 
	}

	private Integer getRandomPort() {
		Random random = new Random();
		int randomIndex = random.nextInt(serversPorts.size());

		Iterator<Integer> iterator = serversPorts.iterator();

		int currentIndex = 0;
		Integer randomElement = null;

		// iterate the HashSet
		while (iterator.hasNext()) {

			randomElement = iterator.next();

			// if current index is equal to random number
			if (currentIndex == randomIndex)
				return randomElement;

			// increase the current index
			currentIndex++;
		}

		return randomElement;
	}

	public static void main(String[] args) {
		keyboard = new Scanner(System.in);
		Client client = null;

		while (true) {
			System.out.println("1. INIT\n2. PUT\n3. GET");

			keyboard = new Scanner(System.in);
			String option = keyboard.next();

			if (option.isEmpty())
				option = null;

			if (option.contains("1") || option.toUpperCase().contains("INIT")) {
				System.out.println("Informe as portas dos servidores: ");

				// inicializando cliente e referenciando os servidores
				client = new Client();
				client.addServerPort(keyboard.nextInt());
				client.addServerPort(keyboard.nextInt());
				client.addServerPort(keyboard.nextInt());
			} else if (option.contains("2") || option.toUpperCase().contains("PUT")) {
				client.sendPutMessage("key_mock", "value_mock");
			} else if (option.contains("3") || option.toUpperCase().contains("GET")) {

			} else
				continue;

		}
	}
}