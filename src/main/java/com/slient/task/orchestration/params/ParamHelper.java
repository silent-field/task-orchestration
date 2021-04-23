package com.slient.task.orchestration.params;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author gy
 * @version 1.0
 * @date 2021/3/16.
 * @description:
 */
public class ParamHelper {
	private ParamHelper() {

	}

	public static Map<String, String> getParamMap(String params) {
		Map<String, String> map = new LinkedHashMap<>();
		if (StringUtils.isBlank(params)) {
			return map;
		}
		String[] items = params.split(",");
		for (String item : items) {
			String[] keyAndValue = item.split("=");
			map.put(keyAndValue[0], keyAndValue[1]);
		}
		return map;
	}

	public static String getParamString(Map<String, String> paramMap) {
		StringBuilder result = new StringBuilder();
		for (Map.Entry<String, String> entry : paramMap.entrySet()) {
			result.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
		}

		return result.length() == 0 ? "" : result.substring(0, result.length() - 1);
	}
}
