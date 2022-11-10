package application;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

public class Server {

	ServerSocket serverSocket;
	InetAddress ip;
	Integer port;
	Integer leaderPort;
	
	Hashtable<String, List<Object>> register = new Hashtable<String, List<Object>>();
	
	public Server(String ip, Integer port) {
		this.port = port;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			while (true && serverSocket != null) {
				try {
					// recebendo pacote
					Socket client = serverSocket.accept();
					System.out.println("Mensagem recebida");
					getMessage(client);
					
				} catch (IOException e) {
					if(!e.getMessage().toUpperCase().equals("Socket Closed".toUpperCase()))
						e.printStackTrace();
				}
			}
		});

		th.start();
	}

	private void getMessage(Socket socket) throws IOException {
		InputStreamReader is = new InputStreamReader(socket.getInputStream());
		BufferedReader reader = new BufferedReader(is);
		
		OutputStream os = socket.getOutputStream();
		DataOutputStream writer = new DataOutputStream(os);
		
		String receivedMsgJson = reader.readLine();
		Message receivedMsg = Message.fromJson(receivedMsgJson);
		System.out.println(receivedMsg);
		
		// enviando mensagem
		Message message = new Message();
		message.setAsPutOk();
		message.setValue("Object has been saved");
		String msgJson = message.toJson();
		writer.writeBytes(msgJson + "\n");
	}
	
	
	private static Scanner keyboard;

	public static void main(String[] args) {
		
		keyboard = new Scanner(System.in);
		String ip = keyboard.next();
		Integer port = keyboard.nextInt();
		
		Server server = new Server(ip, port);
		System.out.println("Servidor online");
	}
}