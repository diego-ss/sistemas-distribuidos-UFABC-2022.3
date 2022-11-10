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
	
	Hashtable<String, List<Object>> register = new Hashtable<String, List<Object>>();
	
	public Server(String ip, Integer port, Integer leaderPort) {
		this.port = port;
		this.leaderPort = leaderPort;
		createSocket();
	}
	
	private void createSocket() {
		Thread th = new Thread(() -> {
			// canal de comunicação não orientado à conexão
			serverSocket = null;
			try {
				serverSocket = new ServerSocket(port);
			}
			catch (IOException e) {
				e.printStackTrace();
			}

			while (true && serverSocket != null) {
				try {
					// recebendo pacote
					Socket client = serverSocket.accept();
					getMessage(client);
					
				} catch (IOException e) {
					if(!e.getMessage().toUpperCase().equals("Socket Closed".toUpperCase()))
						e.printStackTrace();
				}
			}
		});

		th.start();
	}
	
	private void checkGetMessage(Message message, Socket socket) {
		
	}
	
	private void checkPutMessage(Message message, Socket socket) throws IOException {
		if(iAmLeader()) {
			// tratar mensagem
			register.put(message.getKey(), Arrays.asList(message.getValue(), LocalDateTime.now()));
			
			// enviar replication
			
			// envia PUT_OK
			Message putOkMsg = new Message();
			putOkMsg.setAsPutOk(message.getKey(), message.getValue(), LocalDateTime.now());
			sendMessage(putOkMsg, socket);
		} else {
			Socket s = new Socket("127.0.0.1", leaderPort);
			sendMessage(message, s);
			InputStreamReader is = new InputStreamReader(s.getInputStream());
			BufferedReader reader = new BufferedReader(is);
			String response = reader.readLine();
			Message responseMsg = Message.fromJson(response);
			s.close();
			
			sendMessage(responseMsg, socket);
		}
	}
		
	private Boolean iAmLeader() {
		return leaderPort.equals(port);
	}
	
	private void sendMessage(Message message, Socket originSocket) throws IOException {
		
		OutputStream os = originSocket.getOutputStream();
		
		DataOutputStream writer = new DataOutputStream(os);
		
		String msgJson = message.toJson();
		writer.writeBytes(msgJson + "\n");
	}
	
	private void getMessage(Socket socket) throws IOException {
		InputStreamReader is = new InputStreamReader(socket.getInputStream());
		BufferedReader reader = new BufferedReader(is);
		
		OutputStream os = socket.getOutputStream();
		DataOutputStream writer = new DataOutputStream(os);
		
		String receivedMsgJson = reader.readLine();
		Message receivedMsg = Message.fromJson(receivedMsgJson);
		System.out.println("Mensagem recebida: " + receivedMsg);
		
		if(receivedMsg.getType() == MessageType.PUT)
			checkPutMessage(receivedMsg, socket);
		else if(receivedMsg.getType() == MessageType.GET)
			checkGetMessage(receivedMsg, socket);
	}
	
	
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
