package application;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;
import com.google.gson.Gson;

public class Message {
	public enum MessageType {
		// mensagens de requisição
		PUT,
		GET,
		REPLICATION,
		REPLICATION_OK,
		REPLICATION_NOK,
		// mensagens de resposta
		PUT_OK,
		PUT_NOK,
		TRY_OTHER_SERVER_OR_LATER
	}
	
	private MessageType type;
	private UUID id;
	private String key;
	private String value;
	private String timeStamp;
	private Integer clientPort;
	
	public Message() {
		this.id = UUID.randomUUID();
	}
	
	public static String timeStampToString(LocalDateTime timeStamp) {
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		return timeStamp.format(formatter);
	}
	
	public static LocalDateTime stringToDateTime(String timeStampString) {
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		return LocalDateTime.parse(timeStampString, formatter);
	}
	
	public void setAsPut(String key, String value) {
		this.type = MessageType.PUT;
		this.key = key;
		this.value = value;
	}
	
	public void setAsPutOk(String key, String value, LocalDateTime timeStamp) {
		this.type = MessageType.PUT_OK;		
		this.key = key;
		this.value = value;		
		this.timeStamp = timeStampToString(timeStamp);
	}
	
	public void setAsGet(String key, String timeStamp) {
		this.type = MessageType.GET;
		this.key = key;
	}
	
	public void setAsReplication(String key, String value, LocalDateTime timeStamp) {
		this.type = MessageType.REPLICATION;
		this.key = key;
		this.value = value;
		this.timeStamp = timeStampToString(timeStamp);
	}
	
	public void setAsReplicationOK(String key, String value, LocalDateTime timeStamp) {
		this.type = MessageType.REPLICATION_OK;
		this.key = key;
		this.value = value;
		this.timeStamp = timeStampToString(timeStamp);
	}
	
	public void setAsReplicationNOK(String key, String value, LocalDateTime timeStamp) {
		this.type = MessageType.REPLICATION_NOK;
		this.key = key;
		this.value = value;
		this.timeStamp = timeStampToString(timeStamp);
	}
	
	public UUID getId() {
		return id;
	}


	public void setId(UUID id) {
		this.id = id;
	}


	public String getKey() {
		return key;
	}


	public void setKey(String key) {
		this.key = key;
	}
	

	public String getValue() {
		return value;
	}


	public void setValue(String value) {
		this.value = value;
	}

	public LocalDateTime getTimeStamp() {
		return stringToDateTime(this.timeStamp);
	}


	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}


	public MessageType getType() {
		return type;
	}


	public void setType(MessageType type) {
		this.type = type;
	}


	public Integer getClientPort() {
		return clientPort;
	}

	public void setClientPort(Integer clientPort) {
		this.clientPort = clientPort;
	}

	public String toJson() {
		Gson gson = new Gson();
		String json = gson.toJson(this);
		return json;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		return Objects.equals(id, other.id);
	}
	
	@Override
	public String toString() {
		return "Message [type=" + type + ", id=" + id + ", key=" + key + ", value=" + value + ", timeStamp=" + timeStamp + "]";
	}


	public static Message fromJson(String json) {
		Gson gson = new Gson();
		Message message = gson.fromJson(json, Message.class);
		return message;
	}	
}


