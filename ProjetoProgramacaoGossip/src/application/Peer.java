package application;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import application.Message.MessageType;

public class Peer {

	// region MAIN METHODS ---------------------------------------------------------------------------------------------------------------
	private static Scanner keyboard;
	public static void main(String args[]) throws Exception {

		Peer peer = null;

		while (true) {
			// printando opções
			System.out.println();
			System.out.println("1. INICIALIZA");
			System.out.println("2. SEARCH");
			
			// capturando opção selecionada
			keyboard = new Scanner(System.in);
			String option = keyboard.next();

			if (option.isEmpty())
				option = null;

			if (option.contains("1") || option.toUpperCase().contains("INICIALIZA"))
				option = "INICIALIZA";

			if (option.contains("2") || option.toUpperCase().contains("SEARCH"))
				option = "SEARCH";

			if (option == null)
				continue;
			
			// INICIALIZA -------------
			if (option == "INICIALIZA") {
				
				if(peer != null) {
					peer.closeSocket();
					peer = null;
				}
				
				// ler ip
				String address = "";
				while (address == null || address.isEmpty()) {
					System.out.print("Informe o IP: ");
					address = keyboard.next();
				}

				// ler porta
				String port = "";
				while (port == null || port.isEmpty()) {
					System.out.print("Informe a Porta: ");
					port = keyboard.next();
				}

				// ler a pasta dos arquivos
				String filesFolder = "";
				System.out.print("Informe o diretório dos arquivos: ");
				while (filesFolder == null || filesFolder.isEmpty() || Files.notExists(Paths.get(filesFolder))) {
					filesFolder = keyboard.nextLine();
				}

				// ler porta de peer vizinho
				String port1 = "";
				while (port1 == null || port1.isEmpty()) {
					System.out.print("Informe a porta de outro peer (1): ");
					port1 = keyboard.next();
				}

				// ler porta de peer vizinho
				String port2 = "";
				while (port2 == null || port2.isEmpty()) {
					System.out.print("Informe a porta de outro peer (2): ");
					port2 = keyboard.next();
				}

				// inicializa peer
				peer = new Peer(Integer.parseInt(port), address, filesFolder);
					
				// cria o socket
				try {
					peer.createSocket();
				} catch (Exception e) {
					System.out.println("Falha ao inicializar Socket do Peer " + address + " | " + port);
					throw e;
				}

				// lista arquivos do diretório
				peer.setFilesPaths();
				// adiciona os vizinhos
				peer.addNeighbors(Arrays.asList(new Integer[] { Integer.parseInt(port1), Integer.parseInt(port2) }));
			}
			// SEARCH --------------------------
			else if (option == "SEARCH") {
				// ler nome do arquivo buscado
				String fileName = "";
				while (fileName == null || fileName.isEmpty()) {
					System.out.print("Informe o nome do arquivo desejado: ");
					fileName = keyboard.nextLine();
				}
				
				// criando e enviando mensagem de busca
				Message searchMessage = new Message(fileName, MessageType.SEARCH);
				searchMessage.setOriginAddress(peer.getAddress());
				searchMessage.setOriginPort(peer.getPort());
				
				byte[] sendData = new byte[1024];
				sendData = searchMessage.toJson().getBytes();
				peer.forwardMessageToNeighbor(searchMessage);
			}
		}
	}
	// endregion ---------------------------------------------------------------------------------------------------------------

	// region PEER CLASS DEFINITION
	private Integer port; // porta
	private String address; // ip

	private String filesFolder; // diretório de arquivos
	private List<String> filesPaths = new ArrayList<String>(); // lista de arquivos
	private List<Peer> neighbors = new ArrayList<Peer>(); // peers vizinhos
	private List<Message> receivedMessagesControl = new ArrayList<Message>(); // controle de mensagens recebidas
	private Map<Message, LocalDateTime> sentMessagesControl = new HashMap<Message, LocalDateTime>(); // controle de mensagens enviadas
	// a ideia desse map é armazenar a mensagem enviada e o horário, para calcular o timeout de forma schedulada para cada mensagem de forma independente

	private DatagramSocket serverSocket;
	private final long TIMEOUT_SECONDS = 7; // X segundos de espera para timeout
	private Timer timerFiles;
	private Timer timerTimeOut;
	
	// construtor com argumentos
	public Peer(Integer port, String address, String filesFolder) {
		this.port = port;
		this.address = address;
		this.filesFolder = filesFolder;

		// Scheduler de tarefa de atualização de arquivos
		timerFiles = new Timer();
		timerFiles.schedule(new TimerTask() {
			@Override
			public void run() {
				String paths = "";
				setFilesPaths();

				if (filesPaths != null && filesPaths.size() > 0)
					paths = String.join(" | ", filesPaths);

				System.out.println(String.format("Sou peer %s:%s com arquivos %s", address, port, paths));
			}
		}, 0, 1000 * 30);
	
		// Scheduler para verificação de timeout a cada 1 segundo
		timerTimeOut = new Timer();
		timerTimeOut.schedule(new TimerTask() {
			@Override
			public void run() {
				// percorre o MAP de controle de mensagens enviadas
				for(Message message: sentMessagesControl.keySet()) {
					// calcula há quanto tempo a mensagem foi enviada
					long dateDiff = sentMessagesControl.get(message).until(LocalDateTime.now(), ChronoUnit.SECONDS);
					// se superou o tempo do timeout, imprime a mensagem, remove do controle de envio e insere no controle de mensagens tratadas
					if(dateDiff >= TIMEOUT_SECONDS) {
						System.out.println("Ninguém no sistema possui o arquivo " + message.getContent());
						sentMessagesControl.remove(message);
						receivedMessagesControl.add(message);
					}
				}
			}
		}, 0, 1000);
	
	}

	// construtor padrão
	public Peer(Integer port) {
		this.port = port;
	}
	
	// destrói o peer e as conexões/threads por ele criadas
	public void closeSocket() {
		serverSocket.close();
		serverSocket = null;
		timerFiles.cancel();
		timerTimeOut.cancel();
	}

	// cria o socket que vai recepcionar as mensagens
	public void createSocket() throws Exception {
		Thread th = new Thread(() -> {
			// canal de comunicação não orientado à conexão
			serverSocket = null;
			try {
				serverSocket = new DatagramSocket(port);
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println("Peer inicializado.");

			while (true && serverSocket != null) {
				// buffer de recebimento
				byte[] recBuffer = new byte[1024];
				// datagrama que será recebido
				DatagramPacket recPacket = new DatagramPacket(recBuffer, recBuffer.length);

				try {
					// recebendo pacote
					serverSocket.receive(recPacket);
					String informacao = new String(recPacket.getData(), recPacket.getOffset(), recPacket.getLength());
					// mensagem recebida no pacote
					Message message = Message.fromJson(informacao);
					// verifica se a mensagem já não foi tratada
					if (receivedMessagesControl.stream().anyMatch(m -> m.getId().equals(message.getId())))
						continue;
						
					receivedMessagesControl.add(message);

					// criando mensagem de resposta
					Message responseMessage = new Message();
					
					// Se é uma mensagem de busca...
					if (message.getType() == MessageType.SEARCH) {
						// verifica se o arquivo existe no diretório
						Boolean fileExists = fileExistsInFolder(message.getContent());
						// se existe, envia para o peer de origem
						if (fileExists) {
							answerToOriginPeer(message);
							// remove a mensagem da lista de tratadas depois de um tempo
							Timer tempTimer = new Timer();
							tempTimer.schedule(new TimerTask() {
								@Override
								public void run() {
									receivedMessagesControl.remove(message);
									tempTimer.cancel();
								}}, 0, 1000);
						} else {
							// se não existe, passa para os vizinhos
							forwardMessageToNeighbor(message);
						}
					}
					// se é uma mensagem de resposta, apenas printa e faz gestão da fila de mensagens enviadas
					else {
						String responseText = String.format("Peer com o arquivo procurado: %s:%s %s",
								recPacket.getAddress(), recPacket.getPort(), message.getContent());
						System.out.println(responseText);
						sentMessagesControl.remove(message);
					}

				} catch (IOException e) {
					if(!e.getMessage().toUpperCase().equals("Socket Closed".toUpperCase()))
						e.printStackTrace();
				}
			}
		});

		th.start();
	}

	// método para responder ao peer de origin da solicitação
	private void answerToOriginPeer(Message message) throws IOException {
		Message responseMessage = new Message();
		String responseText = String.format("Tenho %s. Respondendo para %s:%s", message.getContent(),
				message.getOriginAddress(), message.getOriginPort());
		System.out.println(responseText);
		// formatando mensagem de resposta
		responseMessage.setContent(message.getContent());
		responseMessage.setType(MessageType.RESPONSE);
		responseMessage.setOriginAddress(message.getOriginAddress());
		responseMessage.setOriginPort(message.getOriginPort());
		responseMessage.setId(message.getId());

		// buffer de resposta
		byte[] sendBuffer = new byte[1024];
		sendBuffer = responseMessage.toJson().getBytes();
		// pacote da resposta
		DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length,
				InetAddress.getByName(message.getOriginAddress()), message.getOriginPort());
		serverSocket.send(sendPacket);
	}

	// método GOSSIP de proparação da mensagem, selecionando aleatoriamente o próximo peer comunicado
	public void forwardMessageToNeighbor(Message message) {

		Thread th_forward = new Thread(() -> {
			// lista para mapear peers não comunicados
			List<Peer> checkedPeers = new ArrayList<Peer>(neighbors);

			// varrendo vizinhos
			while (checkedPeers.size() > 0) {

				// pegando peer randômico e adicionando na lista de visitados
				Random rand = new Random();
				Peer randNeighbor = checkedPeers.get(rand.nextInt(checkedPeers.size()));
				checkedPeers.remove(randNeighbor);
				
				// não redireciona se o vizinho aleatório é o peer de origem
				if(randNeighbor.getPort().equals(message.getOriginPort()))
					continue;

				InetAddress IPAddress = null;
				// criando datagram socket
				try {
					IPAddress = InetAddress.getByName(randNeighbor.getAddress());
				} catch (UnknownHostException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				// criando packet
				byte[] sendData = new byte[1024];
				sendData = message.toJson().getBytes();
				DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, randNeighbor.port);
				// enviando packet
				try {
					
					if(!this.port.equals(message.getOriginPort())) {
						String responseText = String.format("não tenho %s. Encaminhando para %s:%s", message.getContent(),
								InetAddress.getByName(randNeighbor.getAddress()), randNeighbor.getPort());
						System.out.println(responseText);
					}
					
					serverSocket.send(sendPacket);
					// inserindo mensagem no controle de mensagens já tratadas
					sentMessagesControl.put(message, LocalDateTime.now());

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});

		th_forward.start();
	}

	private Boolean fileExistsInFolder(String file) {
		file = new String(file);
		return String.join(" ", filesPaths).contains(file);
	}

	// GETTERS E SETTERS
	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getFilesFolder() {
		return filesFolder;
	}

	public void setFilesFolder(String filesFolder) {
		this.filesFolder = filesFolder;
	}

	public List<String> getFilesPaths() {
		return filesPaths;
	}

	// Preenche a lista de arquivos do diretório
	public void setFilesPaths() {
		filesPaths = new ArrayList<String>();
		File file = new File(filesFolder);
		for(File f: file.listFiles())
			this.filesPaths.add(f.getName());
	}

	public List<Peer> getNeighbors() {
		return neighbors;
	}

	public void addNeighbors(Peer peer) {
		this.neighbors.add(peer);
	}

	public void setFilesPaths(List<String> filesPaths) {
		this.filesPaths = filesPaths;
	}

	public void setNeighbors(List<Peer> neighbors) {
		this.neighbors = neighbors;
	}

	public void addNeighbors(List<Integer> ports) {
		for (Integer port : ports) {
			Peer peer = new Peer(port);
			this.neighbors.add(peer);
		}
	}
	// endregion
}
