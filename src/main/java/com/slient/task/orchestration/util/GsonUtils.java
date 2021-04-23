package com.slient.task.orchestration.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * @author gy
 * GSON 工具类
 */
public class GsonUtils {
	/**
	 * 默认序列化null值
	 */
	private static final Gson GSON_DEFAULT = createGson(true, false);
	/**
	 * 不序列化null值
	 */
	private static final Gson GSON_WITHOUT_NULLS = createGson(false, false);
	/**
	 * 打印格式化 json 字符串，同时序列化 null 值
	 */
	private static final Gson GSON_PRETTY_PRINT = createGson(true, true);

	private GsonUtils() {
	}

	private static final Gson createGson(boolean serializeNulls, boolean prettyPrint) {
		GsonBuilder builder = new GsonBuilder();
		/**
		 * 对{@link java.util.Date}, {@link java.sql.Timestamp} , {@link java.sql.Date} 按照 yyyy-MM-dd HH:mm:ss 格式序列化反序列化
		 */
		builder.setDateFormat("yyyy-MM-dd HH:mm:ss");
//		builder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
		if (serializeNulls) {
			builder.serializeNulls();
		}

		if (prettyPrint) {
			builder.setPrettyPrinting();
		}

		return builder.create();
	}

	public static final Gson getGson() {
		return GSON_DEFAULT;
	}

	public static final Gson getGson(boolean serializeNulls) {
		return serializeNulls ? GSON_DEFAULT : GSON_WITHOUT_NULLS;
	}

	/**
	 * 默认不序列化null对象
	 *
	 * @param object
	 * @return
	 */
	public static final String toJson(Object object) {
		return GSON_DEFAULT.toJson(object);
	}

	public static final String toJson(Object object, boolean serializeNulls) {
		return getGson(serializeNulls).toJson(object);
	}

	public static final <T> T fromJson(String json, Class<T> type) {
		return GSON_DEFAULT.fromJson(json, type);
	}

	public static final <T> T fromJson(String json, Type type) {
		return GSON_DEFAULT.fromJson(json, type);
	}

	public static final <T> T fromJson(JsonElement jsonElement, Class<T> type) {
		return GSON_DEFAULT.fromJson(jsonElement, type);
	}

	public static final <T> T fromJson(JsonElement jsonElement, Type type) {
		return GSON_DEFAULT.fromJson(jsonElement, type);
	}

	public static String toPrettyPrintJson(Object o) {
		return GSON_PRETTY_PRINT.toJson(o);
	}

	public Boolean isValidJson(String str) {
		try {
			GSON_DEFAULT.fromJson(str, Object.class);
			return true;
		} catch (JsonSyntaxException e) {
			return false;
		}
	}

	public static JsonElement parseJsonElement(String json) {
		return JsonParser.parseString(json);
	}

	/**
	 * json 数组 转成 特定的 class list
	 *
	 * @param json
	 * @param clz
	 * @return
	 */
	public static <T> List<T> fromJsonList(String json, Class<T> clz) {
		return GSON_DEFAULT.fromJson(
				json,
				new TypeToken<List<T>>() {
				}.getType()
		);
	}
}