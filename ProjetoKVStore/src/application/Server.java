package application;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import com.google.gson.Gson;

import application.Message.MessageType;

public class Server {

	ServerSocket serverSocket;
	InetAddress ip;
	Integer port;
	Integer leaderPort;
	
	// hash table no formato 
	// (key (string), { value (string), timeStamp (localdatetime)})
	Hashtable<String, List<Object>> register = new Hashtable<String, List<Object>>();
	
	
	public Server(String ip, Integer port, Integer leaderPort) {
		this.port = port;
		this.leaderPort = leaderPort;
		createSocket();
	}
	
	/**
	 * Cria o Socket de comunicação do servidor instanciado
	 * O recebimento de mensagens é assíncrono
	 */
	private void createSocket() {
		Thread th = new Thread(() -> {
			// canal de comunicação não orientado à conexão
			serverSocket = null;
			try {
				// criando socket
				serverSocket = new ServerSocket(port);
			}
			catch (IOException e) {
				e.printStackTrace();
			}

			while (true && serverSocket != null) {
				try {
					// recebendo pacote
					Socket client = serverSocket.accept();
					// capturando mensagem  
					getMessage(client);
					
				} catch (IOException e) {
					if(!e.getMessage().toUpperCase().equals("Socket Closed".toUpperCase()))
						e.printStackTrace();
				}
			}
		});

		th.start();
	}
	
	/**
	 * Trata mensagens do tipo GET.
	 * @param message - Mensagem recebida do client
	 * @param clientSocket - Socket de comunicação com o client ou servidor
	 * @throws IOException
	 */
	private void checkGetMessage(Message message, Socket socket) {
		
	}
	
	/**
	 * Trata mensagens do tipo PUT. Caso seja o líder, salva os dados.Caso contrário,
	 * redirecionado ao líder
	 * @param message - Mensagem recebida do client
	 * @param clientSocket - Socket de comunicação com o client ou servidor
	 * @throws IOException
	 */
	private void checkPutMessage(Message message, Socket clientSocket) throws IOException {
		if(iAmLeader()) {
			// registra key, value e time stamp
			List<Object> values = Arrays.asList(message.getValue(), LocalDateTime.now());
			register.put(message.getKey(), values);
			
			// enviar replication
			//Boolean successfullyReplicated = replicateToServers(message.getKey(), values);
			Boolean successfullyReplicated = true;
			if(successfullyReplicated) {
				// envia PUT_OK
				Message putOkMsg = new Message();
				putOkMsg.setAsPutOk(message.getKey(), message.getValue(), LocalDateTime.now());
				sendMessage(putOkMsg, clientSocket);
			} else {
				System.out.println("Failed to replicate objects");
			}

		} else {
			// redirecionando PUT para o líder
			Socket serverToLeaderSocket = new Socket("127.0.0.1", leaderPort);
			sendMessage(message, serverToLeaderSocket);
			//serverToLeaderSocket.close();

			// stream de leitura para aguardar resposta do líder
			InputStreamReader is = new InputStreamReader(serverToLeaderSocket.getInputStream());
			BufferedReader reader = new BufferedReader(is);
			String response = reader.readLine();
			// criando json da mensagem
			Message responseMsg = Message.fromJson(response);
			
			// redirecionando mensagem PUT_OK do líder para o client
			sendMessage(responseMsg, clientSocket);
		}
	}
	
	private void checkReplicationMessage(Message receivedMsg, Socket socket) throws IOException {
		
		try {
			register.put(receivedMsg.getKey(), Arrays.asList(receivedMsg.getValue(), receivedMsg.getTimeStamp()));
			Message message = new Message();
			message.setAsReplicationOK(receivedMsg.getKey(), receivedMsg.getValue(), receivedMsg.getTimeStamp());
			sendMessage(message, socket);
			
		} catch (Exception ex) {
			Message message = new Message();
			message.setAsReplicationNOK(receivedMsg.getKey(), receivedMsg.getValue(), receivedMsg.getTimeStamp());
			sendMessage(message, socket);
			System.out.println(ex.getMessage());
		}
	}
		
	private Boolean replicateToServers(String key, List<Object> values) throws IOException {
		Set<Integer> serversPorts = new HashSet<>();
		serversPorts.add(10097);
		serversPorts.add(10098);
		serversPorts.add(10099);
		
		for(Integer port: serversPorts) {
			if(!port.equals(this.port)) {
				Message message = new Message();
				message.setAsReplication(key, 
						(String)values.get(0), 
						(LocalDateTime)values.get(1));
				
				// redirecionando para outros servidores
				Socket serverSocket = new Socket("127.0.0.1", port);
				sendMessage(message, serverSocket);
				
				// stream de leitura para aguardar resposta 
				InputStreamReader is = new InputStreamReader(serverSocket.getInputStream());
				BufferedReader reader = new BufferedReader(is);
				String response = reader.readLine();
				
				// criando json da mensagem
				Message responseMsg = Message.fromJson(response);
				
				// se qualquer um dos servidores responder como NOK, retorna falso
				if(responseMsg.getType() == MessageType.REPLICATION_NOK)
					return false;
				
				System.out.println("Replicado com sucesso para [" + port + "]");
				serverSocket.close();
			}
		}
		
		return true;
	}
	
	/**
	 * Verifica se a instância de servidor atual é o líder
	 * @return Boolean true ou false
	 */
	private Boolean iAmLeader() {
		return leaderPort.equals(port);
	}
	
	/**
	 * Envia uma mensagem ao socket informado
	 * @param message - Mensagem a ser enviada
	 * @param destSocket - Socket de destino
	 * @throws IOException
	 */
	private void sendMessage(Message message, Socket destSocket) throws IOException {
		// stream de escrita no socket de origem
		OutputStream os = destSocket.getOutputStream();
		DataOutputStream writer = new DataOutputStream(os);
		// convertendo mensagem para json
		String msgJson = message.toJson();
		// enviando mensagem
		writer.writeBytes(msgJson + "\n");
	}
	
	/**
	 * Captura mensagens recebidas no socket do servidor instanciado
	 * @param socket - Socket de origem da mensagem recebida
	 * @throws IOException
	 */
	private void getMessage(Socket socket) throws IOException {
		// stream de leitura
		InputStreamReader is = new InputStreamReader(socket.getInputStream());
		BufferedReader reader = new BufferedReader(is);
		
		// stream de escrita
		OutputStream os = socket.getOutputStream();
		DataOutputStream writer = new DataOutputStream(os);
		
		// convertendo json em mensagem
		String receivedMsgJson = reader.readLine();
		Message receivedMsg = Message.fromJson(receivedMsgJson);
		System.out.println("Mensagem recebida: " + receivedMsg);
		
		if(receivedMsg.getClientPort() == null)
			receivedMsg.setClientPort(socket.getPort());
		
		String test = socket.getRemoteSocketAddress().toString().substring(1);

		// tratando as mensagens por tipo
		if(receivedMsg.getType() == MessageType.PUT)
			checkPutMessage(receivedMsg, socket);
		else if(receivedMsg.getType() == MessageType.GET)
			checkGetMessage(receivedMsg, socket);
		else if(receivedMsg.getType() == MessageType.REPLICATION)
			checkReplicationMessage(receivedMsg, socket);
		else
			return;
	}
	
	// MÉTODO MAIN E DEPENDÊNCIAS --------------------------------------

	private static Scanner keyboard;

	public static void main(String[] args) {
		
		keyboard = new Scanner(System.in);
		System.out.println("Informe o IP: ");
		String ip = keyboard.next();
		System.out.println("Informe a porta: ");
		Integer port = keyboard.nextInt();
		
		System.out.println("Informe a porta do líder: ");
		Integer leaderPort = keyboard.nextInt();

		
		new Server(ip, port, leaderPort);
		System.out.println("Servidor online");
		
		
	}
}
