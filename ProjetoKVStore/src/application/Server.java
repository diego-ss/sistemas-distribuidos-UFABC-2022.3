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
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

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
			register.put(message.getKey(), Arrays.asList(message.getValue(), LocalDateTime.now()));
			
			// TODO - enviar replication
			
			// envia PUT_OK
			Message putOkMsg = new Message();
			putOkMsg.setAsPutOk(message.getKey(), message.getValue(), LocalDateTime.now());
			sendMessage(putOkMsg, clientSocket);
		} else {
			// redirecionando PUT para o líder
			Socket serverToLeaderSocket = new Socket("127.0.0.1", leaderPort);
			sendMessage(message, serverToLeaderSocket);
			
			// stream de leitura para aguardar resposta do líder
			InputStreamReader is = new InputStreamReader(serverToLeaderSocket.getInputStream());
			BufferedReader reader = new BufferedReader(is);
			String response = reader.readLine();
			// criando json da mensagem
			Message responseMsg = Message.fromJson(response);
			serverToLeaderSocket.close();
			
			// redirecionando mensagem PUT_OK do líder para o client
			sendMessage(responseMsg, clientSocket);
		}
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
		
		// tratando as mensagens por tipo
		if(receivedMsg.getType() == MessageType.PUT)
			checkPutMessage(receivedMsg, socket);
		else if(receivedMsg.getType() == MessageType.GET)
			checkGetMessage(receivedMsg, socket);
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
