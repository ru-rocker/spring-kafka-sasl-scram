package com.rurocker.demo.kafkasecurity.util;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.rurocker.demo.kafkasecurity.sec.AES;

public class AESDeserializer<T> extends JsonDeserializer<T> {

	public static final String AES_SECRET_KEY = "aes.deserializer.secret.keys";
	public static final String AES_SALT_KEY = "aes.deserializer.salt.keys";

	private String secret = null;
	private String salt = null;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		super.configure(configs, isKey);
		secret = (String) configs.get(AES_SECRET_KEY);
		if (secret == null) {
			throw new SerializationException(AES_SECRET_KEY + " cannot be null.");
		}

		salt = (String) configs.get(AES_SALT_KEY);
		if (salt == null) {
			throw new SerializationException(AES_SALT_KEY + " cannot be null.");
		}
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		return super.deserialize(topic, headers, AES.decrypt(data, secret, salt));
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		return super.deserialize(topic, AES.decrypt(data, secret, salt));
	}

}
