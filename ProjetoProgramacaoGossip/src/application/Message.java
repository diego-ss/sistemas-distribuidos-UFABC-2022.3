package application;

import java.util.Objects;
import java.util.UUID;

import com.google.gson.Gson;

public class Message {

	public enum MessageType{
		SEARCH,
		RESPONSE
	}
	
	private UUID id;
	private String content;
	private MessageType type;
	
	private String originAddress;
	private Integer originPort;
	

	public Message() {
		initializeUUID();
	}

	public Message(String content, MessageType type) {
		initializeUUID();
		this.type = type;
		this.content = content;
	}
	
	private void initializeUUID() {
		if(this.getId() == null)
			this.setId(UUID.randomUUID());
	}


	public String toJson() {
		Gson gson = new Gson();
		String json = gson.toJson(this);
		return json;
	}
	
	
	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getContent() {
		return content;
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

	public void setContent(String content) {
		this.content = content;
	}
	
	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}
	
	public String getOriginAddress() {
		return originAddress;
	}

	public void setOriginAddress(String originAddress) {
		this.originAddress = originAddress;
	}

	public Integer getOriginPort() {
		return originPort;
	}

	public void setOriginPort(Integer originPort) {
		this.originPort = originPort;
	}

	public static Message fromJson(String json) {
		Gson gson = new Gson();
		Message message = gson.fromJson(json, Message.class);
		return message;
	}	
}
