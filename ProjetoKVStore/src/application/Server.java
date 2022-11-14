package application;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
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

	public Server(String ip, Integer port, Integer leaderPort) throws UnknownHostException {
		this.port = port;
		this.leaderPort = leaderPort;
		this.ip = InetAddress.getByName(ip);
		createSocket();
	}

	/**
	 * Cria o Socket de comunicação do servidor instanciado O recebimento de
	 * mensagens é assíncrono
	 */
	private void createSocket() {
		Thread th = new Thread(() -> {
			// canal de comunicação não orientado à conexão
			serverSocket = null;
			try {
				// criando socket
				serverSocket = new ServerSocket(port);
			} catch (IOException e) {
				e.printStackTrace();
			}

			while (true && serverSocket != null) {
				try {
					// recebendo pacote
					Socket client = serverSocket.accept();
					// capturando mensagem
					getMessage(client);

				} catch (IOException e) {
					if (!e.getMessage().toUpperCase().equals("Socket Closed".toUpperCase()))
						e.printStackTrace();
				}
			}
		});

		th.start();
	}
	
	
	/**
	 * Captura mensagens recebidas no socket do servidor instanciado
	 * 
	 * @param socket - Socket de origem da mensagem recebida
	 * @throws IOException
	 */
	private void getMessage(Socket socket) throws IOException {
		Thread th = new Thread(() -> {

			try {
				// stream de leitura
				InputStreamReader is = new InputStreamReader(socket.getInputStream());
				BufferedReader reader = new BufferedReader(is);

				// stream de escrita
				OutputStream os = socket.getOutputStream();
				DataOutputStream writer = new DataOutputStream(os);

				// convertendo json em mensagem
				String receivedMsgJson = receivedMsgJson = reader.readLine();
				Message receivedMsg = Message.fromJson(receivedMsgJson);
				// System.out.println("Mensagem recebida: " + receivedMsg);

				// tratando as mensagens por tipo
				if (receivedMsg.getType() == MessageType.PUT)
					checkPutMessage(receivedMsg, socket);
				else if (receivedMsg.getType() == MessageType.GET)
					checkGetMessage(receivedMsg, socket);
				else if (receivedMsg.getType() == MessageType.REPLICATION)
					checkReplicationMessage(receivedMsg, socket);
				else
					return;
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}

		});

		th.start();
	}


	/**
	 * Trata mensagens do tipo GET.
	 * 
	 * @param message      - Mensagem recebida do client
	 * @param clientSocket - Socket de comunicação com o client ou servidor
	 * @throws IOException
	 */
	private void checkGetMessage(Message message, Socket socket) throws IOException {
		String key = message.getKey();
		Message response = new Message();
		List<Object> objValues = register.get(key);
		
		// verifica se a chave está registrada
		if(objValues != null) {
			// caso esteja registrada, retorna o valor
			LocalDateTime timeStampRef = (LocalDateTime)objValues.get(1);
			
			// indíce randômico para simular falhas
			Double rand = new Random().nextDouble();
			
			// verificando se o registro do servidor é mais antigo ou igual ao do cliente
			if(rand > 0.3 && (timeStampRef.isBefore(message.getTimeStamp()) || timeStampRef.isEqual(message.getTimeStamp()))) {
				// Printando Mensagem do Servidor
				System.out.println(String.format("Cliente 127.0.0.1:%d GET key:'%s' timestamp:'%s'. "
						+ "Meu timestamp é '%s', portanto devolvendo %s",
						message.getClientPort(), message.getKey(), Message.timeStampToString(message.getTimeStamp()),
						Message.timeStampToString((LocalDateTime)objValues.get(1)), (String)objValues.get(0)));
				
				response.setAsGetResponse(key, (String)objValues.get(0), (LocalDateTime)objValues.get(1));

			}
			else {
				// Printando Mensagem do Servidor
				System.out.println(String.format("Cliente 127.0.0.1:%d GET key:'%s' timestamp:'%s'. "
						+ "SIMULAÇÃO DE CHAVE DESATUALIZADA, portanto devolvendo null (TRY_OTHER_SERVER_OR_LATER)",
						message.getClientPort(), message.getKey(), Message.timeStampToString(message.getTimeStamp()),
						Message.timeStampToString((LocalDateTime)objValues.get(1)), (String)objValues.get(0)));
				
				response.setAsTryOtherServerOrLater(key);
			}
			
		} else {
			// caso não esteja registrada, retorna valor nulo
			response.setAsGetResponse(key, null, message.getTimeStamp());
		}
		
		sendMessage(response, socket);
	}

	/**
	 * Trata mensagens do tipo PUT. Caso seja o líder, salva os dados.Caso
	 * contrário, redirecionado ao líder
	 * 
	 * @param message      - Mensagem recebida do client
	 * @param clientSocket - Socket de comunicação com o client ou servidor
	 * @throws IOException
	 */
	private void checkPutMessage(Message message, Socket clientSocket) throws IOException {
		if (iAmLeader()) {
			// Printando Mensagem do Servidor
			System.out.println(String.format("Cliente 127.0.0.1:%d PUT key:'%s' value:'%s'",
					message.getClientPort(), message.getKey(), message.getValue()));
			
			// registra key, value e time stamp
			LocalDateTime registerTime = LocalDateTime.now();
			List<Object> values = Arrays.asList(message.getValue(), registerTime);
			register.put(message.getKey(), values);

			// enviar replication
			Boolean successfullyReplicated = replicateToServers(message.getKey(), values);
			Message putMsg = new Message();

			//Boolean successfullyReplicated = true;
			if (successfullyReplicated) {
				// Printando Mensagem do Servidor
				System.out.println(String.format("Enviando PUT_OK ao Cliente 127.0.0.1:%d da key:'%s' timestamp:'%s'",
						message.getClientPort(), message.getKey(),  Message.timeStampToString(message.getTimeStamp())));
				
				// envia PUT_OK
				putMsg.setAsPutOk(message.getKey(), message.getValue(), registerTime);
				sendMessage(putMsg, clientSocket);
			} else {
				// envia PUT_NOK
				putMsg.setAsPutNOk(message.getKey(), message.getValue());
				register.remove(message.getKey());
				sendMessage(putMsg, clientSocket);
				System.out.println("Failed to replicate objects");
			}

		} else {
			// Printando Mensagem do Servidor
			System.out.println(String.format("Encaminhando PUT key:'%s' value:'%s'",
					message.getKey(), message.getValue()));
			
			// redirecionando PUT para o líder
			Socket serverToLeaderSocket = new Socket("127.0.0.1", leaderPort);
			sendMessage(message, serverToLeaderSocket);
			// serverToLeaderSocket.close();

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

	/**
	 * Trata mensagens do tipo Replication recebidas
	 * @param receivedMsg - mensagem recebida
	 * @param socket - socket de origem da mensagem
	 * @throws IOException
	 */
	private void checkReplicationMessage(Message receivedMsg, Socket socket) throws IOException {

		try {
			// Printando Mensagem do Servidor
			System.out.println(String.format("REPLICATION key:'%s' value:'%s' timestamp: '%s'",
					receivedMsg.getKey(), receivedMsg.getValue(), Message.timeStampToString(receivedMsg.getTimeStamp())));
			
			// registra a key e os valores no register
			register.put(receivedMsg.getKey(), Arrays.asList(receivedMsg.getValue(), receivedMsg.getTimeStamp()));
			Message message = new Message();
			// enviando responsta
			message.setAsReplicationOK(receivedMsg.getKey(), receivedMsg.getValue(), receivedMsg.getTimeStamp());
			sendMessage(message, socket);

		} catch (Exception ex) {
			// em caso de falha, envia REPLICATION_NOK e printa o erro
			Message message = new Message();
			message.setAsReplicationNOK(receivedMsg.getKey(), receivedMsg.getValue(), receivedMsg.getTimeStamp());
			sendMessage(message, socket);
			System.out.println(ex.getMessage());
		}
	}

	/**
	 * Replica um objeto chave-valor para os demais servidores
	 * @param key - chave
	 * @param values - lista de tamanho 2 com valor e timestamp
	 * @return true ou false
	 * @throws IOException
	 */
	private Boolean replicateToServers(String key, List<Object> values) throws IOException {
		Set<Integer> serversPorts = new HashSet<>();
		// portas fixadas hard-coded
		serversPorts.add(10097);
		serversPorts.add(10098);
		serversPorts.add(10099);

		// redirecionando para outros servidores
		for (Integer port : serversPorts) {
			// ignora o próprio líder
			if (!port.equals(this.port)) {
				Message message = new Message();
				message.setAsReplication(key, (String) values.get(0), (LocalDateTime) values.get(1));

				Socket serverSocket = null;
				try {
					// caso não exista servidor em uma das portas, ignora
					serverSocket = new Socket("127.0.0.1", port);
				} catch(Exception ex) {
					continue;
				}
				
				// enviando mensagem
				sendMessage(message, serverSocket);

				// stream de leitura para aguardar resposta
				InputStreamReader is = new InputStreamReader(serverSocket.getInputStream());
				BufferedReader reader = new BufferedReader(is);
				String response = reader.readLine();

				// criando json da mensagem
				Message responseMsg = Message.fromJson(response);

				// se qualquer um dos servidores responder como NOK, retorna falso
				if (responseMsg.getType() == MessageType.REPLICATION_NOK)
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
	 * 
	 * @param message    - Mensagem a ser enviada
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

	
	// MÉTODO MAIN E DEPENDÊNCIAS --------------------------------------

	private static Scanner keyboard;

	public static void main(String[] args) throws Exception {

		keyboard = new Scanner(System.in);
		System.out.println("Informe o IP: ");
		String ip = keyboard.next();
		System.out.println("Informe a porta: ");
		Integer port = keyboard.nextInt();

		System.out.println("Informe a porta do líder: ");
		Integer leaderPort = keyboard.nextInt();

		try {
			new Server(ip, port, leaderPort);
			System.out.println("Servidor online");
		} catch (UnknownHostException e) {
			throw new Exception("Falha ao inicializar servidor: " + e.getMessage());
		}

	}
}
