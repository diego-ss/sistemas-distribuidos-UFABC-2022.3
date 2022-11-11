package application;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

public class Client {

	// relação de portas dos servidores
	private Set<Integer> serversPorts = new HashSet<Integer>();
	// hashtable das chaves registras
	Hashtable<String, LocalDateTime> register = new Hashtable<String, LocalDateTime>();

	private static Scanner keyboard;

	public Client() {

	}
	
	/**
	 * Adiciona portas de servidores à lista
	 * @param port - porta do servidor
	 */
	public void addServerPort(Integer port) {
		serversPorts.add(port);
	}

	/**
	 * Envia uma mensagem do tipo PUT a um servidor aleatória da lista 
	 * De servidores
	 * @param key - chave do novo item
	 * @param value - valor do novo item
	 */
	public void sendPutMessage(String key, String value) {
		Thread th = new Thread(() -> {
			// criando mensagem
			Message message = new Message();
			message.setAsPut(key, value);
			
			// aguardando resposta
			Message response = sendMessage(message);
			// registrando key + timestamp
			register.put(response.getKey(), response.getTimeStamp());
			
			for(String k: register.keySet())
				System.out.println(register.get(k));
		});

		th.start();
	}
	
	/**
	 * Envia uma mensagem do tipo GET a um servidor aleatório da lista
	 * @param key - chave do item buscado
	 */
	public void sendGetMessage(String key) {
		Thread th = new Thread(() -> {
			
			if(register.get(key) != null) {
				// criando mensagem
				Message message = new Message();
				message.setAsGet(key, 
						Message.timeStampToString(register.get(key)));
				
				// aguardando resposta
				Message response = sendMessage(message);
			} else {
				System.out.println("The requested resource is not registered");
			}

		});

		th.start();
	}

	/**
	 * Envio de mensagens aos servidores
	 * @param message - mensagem a ser enviada
	 * @return resposta do servidor
	 */
	public Message sendMessage(Message message) {
		try {
			// pegando servidor aleatório
			Integer serverPort = getRandomPort();

			// criando socket e streams de escrita/leitura
			Socket s = new Socket("127.0.0.1", serverPort);
			OutputStream os = s.getOutputStream();
			DataOutputStream writer = new DataOutputStream(os);
			InputStreamReader is = new InputStreamReader(s.getInputStream());
			BufferedReader reader = new BufferedReader(is);

			// enviando mensagem
			message.setClientPort(s.getLocalPort());
			String msgJson = message.toJson();
			writer.writeBytes(msgJson + "\n");
			String response = reader.readLine();
			Message responseMsg = Message.fromJson(response);
			System.out.println(responseMsg);
			s.close();
			return responseMsg;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} 
	}

	/**
	 * Seleciona aleatoriamente uma porta da lista de portas
	 * @return porta de servidor aleatória
	 */
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
	
	// MÉTODO MAIN -------------------------------------------

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
				System.out.print("Informe a chave: ");
				String key = keyboard.next();
				
				System.out.print("\nInforme o valor: ");
				String value = keyboard.next();
				
				client.sendPutMessage(key, value);
			} else if (option.contains("3") || option.toUpperCase().contains("GET")) {
				System.out.print("Informe a chave: ");
				String key = keyboard.next();
				
				client.sendGetMessage(key);
			} else
				continue;

		}
	}
}
