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

import application.Message.MessageType;

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
			if(response.getType() == MessageType.PUT_OK)
				register.put(key, response.getTimeStamp());
			else
				System.out.println("The put request was not successful");
		});

		th.start();
	}
	
	/**
	 * Envia uma mensagem do tipo GET a um servidor aleatório da lista
	 * @param key - chave do item buscado
	 */
	public void sendGetMessage(String key) {
		Thread th = new Thread(() -> {
			
				// criando mensagem
				Message message = new Message();
				message.setAsGet(key, register.get(key));
				
				// aguardando resposta
				Message response = sendMessage(message);

		});

		th.start();
	}

	/**
	 * Envio de mensagens aos servidores
	 * @param message - mensagem a ser enviada
	 * @return resposta do servidor
	 */
	private Message sendMessage(Message message) {
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
			// aguardando o retorno
			String response = reader.readLine();
			Message responseMsg = Message.fromJson(response);
			
			String msgType = MessageType.getName(responseMsg.getType());
			// imprimindo resultado
			System.out.println(String.format("%s key '%s' value '%s' timestamp %s %s no servidor %s:%d",
					msgType, responseMsg.getKey(), responseMsg.getValue(), 
					Message.timeStampToString(responseMsg.getTimeStamp()),
					(msgType == "GET" || msgType == "TRY_OTHER_SERVER_OR_LATER") ? "obtida": "realizada","127.0.0.1", serverPort));
			
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
		// criando random
		Random random = new Random();
		int randomIndex = random.nextInt(serversPorts.size());
		// capturando iterator
		Iterator<Integer> iterator = serversPorts.iterator();

		int currentIndex = 0;
		Integer randomElement = null;

		// iterando o hashset
		while (iterator.hasNext()) {

			randomElement = iterator.next();

			// verificando o index igual ao número randômico
			if (currentIndex == randomIndex)
				return randomElement;

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
				// captura informações do teclado e envia mensagem de put
				System.out.print("Informe a chave: ");
				keyboard.nextLine();
				String key = keyboard.nextLine();
				
				System.out.print("\nInforme o valor: ");
				String value = keyboard.nextLine();
				
				client.sendPutMessage(key, value);
			} else if (option.contains("3") || option.toUpperCase().contains("GET")) {
				// captura informações do teclado e envia mensagem de get
				System.out.print("Informe a chave: ");
				keyboard.nextLine();
				String key = keyboard.nextLine();
				
				client.sendGetMessage(key);
			} else
				continue;

		}
	}
}
