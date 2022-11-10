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
		// mensagens de resposta
		PUT_OK,
		PUT_NOK
	}
	
	private MessageType type;
	private UUID id;
	private String key;
	private String value;
	private String timeStamp;
	
	public Message() {
		this.id = UUID.randomUUID();
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
		
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		this.timeStamp = timeStamp.format(formatter);
	}
	
	public void setAsGet(String key) {
		this.type = MessageType.GET;
		this.key = key;
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

	public String getTimeStamp() {
		return timeStamp;
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


