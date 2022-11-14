package application;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;
import com.google.gson.Gson;

public class Message {
	public enum MessageType {
		// mensagens de requisição
		PUT("PUT"),
		GET("GET"),
		REPLICATION("REPLICATION"),
		// mensagens de resposta
		GET_RESPONSE("GET"),
		PUT_OK("PUT_OK"),
		PUT_NOK("PUT_NOK"),
		TRY_OTHER_SERVER_OR_LATER("TRY_OTHER_SERVER_OR_LATER"),
		REPLICATION_OK("REPLICATION_OK"),
		REPLICATION_NOK("REPLICATION_NOK");
		
	    private final String name;       

	    private MessageType(String s) {
	        name = s;
	    }
	    
	    public String toString() {
	        return this.name;
	     }
	    
	    public boolean equalsName(String otherName) {
	        return name.equals(otherName);
	    }
	    
	    public static String getName(MessageType type) {
	    	for(MessageType t: MessageType.values()) {
	    		if(type.equals(t))
	    			return t.toString();
	    	}
	    	return null;
	    }
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
		if(timeStamp == null)
			return null;
		
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		return timeStamp.format(formatter);
	}
	
	public static LocalDateTime stringToDateTime(String timeStampString) {
		if(timeStampString == null)
			return null;
		
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		return LocalDateTime.parse(timeStampString, formatter);
	}
	
	public void setAsPut(String key, String value) {
		this.type = MessageType.PUT;
		this.key = key;
		this.value = value;
	}
	
	public void setAsTryOtherServerOrLater(String key) {
		this.type = MessageType.TRY_OTHER_SERVER_OR_LATER;
		this.key = key;
	}
	
	public void setAsPutOk(String key, String value, LocalDateTime timeStamp) {
		this.type = MessageType.PUT_OK;		
		this.key = key;
		this.value = value;		
		this.timeStamp = timeStampToString(timeStamp);
	}
	
	public void setAsPutNOk(String key, String value) {
		this.type = MessageType.PUT_NOK;		
		this.key = key;
		this.value = value;		
	}
	
	public void setAsGet(String key, LocalDateTime timeStamp) {
		this.type = MessageType.GET;
		this.key = key;
		this.timeStamp = timeStampToString(timeStamp);
	}
	
	public void setAsGetResponse(String key, String value, LocalDateTime timeStamp) {
		this.type = MessageType.GET_RESPONSE;
		this.key = key;
		this.value = value;
		this.timeStamp = timeStampToString(timeStamp);
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


