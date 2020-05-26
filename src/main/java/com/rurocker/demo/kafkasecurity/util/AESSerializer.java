package com.rurocker.demo.kafkasecurity.util;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.rurocker.demo.kafkasecurity.sec.AES;

public class AESSerializer<T> extends JsonSerializer<T> {

	public static final String AES_SECRET_KEY = "aes.serializer.secret.keys";
	public static final String AES_SALT_KEY = "aes.serializer.salt.keys";

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
	public byte[] serialize(String topic, T data) {
		byte[] json = super.serialize(topic, data);
		return AES.encrypt(json, secret, salt);
	}

}
