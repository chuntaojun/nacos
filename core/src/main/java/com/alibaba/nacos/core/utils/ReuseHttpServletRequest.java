/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.utils;

import com.alibaba.nacos.common.http.HttpUtils;
import com.alibaba.nacos.common.http.param.MediaType;
import com.alibaba.nacos.common.utils.ByteUtils;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class ReuseHttpServletRequest extends HttpServletRequestWrapper implements ReuseHttpRequest {

	private byte[] body;
	private Map<String, String[]> stringMap = new HashMap<>(8);
	private final HttpServletRequest target;

	/**
	 * Constructs a request object wrapping the given request.
	 *
	 * @param request The request to wrap
	 * @throws IllegalArgumentException if the request is null
	 */
	public ReuseHttpServletRequest(HttpServletRequest request) throws IOException {
		super(request);
		this.target = request;
		this.body = toBytes(request.getInputStream());
		this.stringMap.putAll(request.getParameterMap());
	}

	@Override
	public Object getBody() throws Exception {
		if (StringUtils.containsIgnoreCase(target.getContentType(), MediaType.MULTIPART_FORM_DATA)) {
			return target.getParts();
		} else {
			String s = ByteUtils.toString(body);
			return StringUtils.isBlank(s) ? HttpUtils.encodingParams(HttpUtils.translateParameterMap(stringMap),
							StandardCharsets.UTF_8.name()) :
					s;
		}
	}

	private byte[] toBytes(InputStream inputStream) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int n = 0;
		while ((n = inputStream.read(buffer)) != -1) {
			bos.write(buffer, 0, n);
		}
		return bos.toByteArray();
	}

	@Override
	public BufferedReader getReader() throws IOException {
		return new BufferedReader(new InputStreamReader(getInputStream()));
	}

	@Override
	public Map<String, String[]> getParameterMap() {
		return super.getParameterMap();
	}

	@Override
	public ServletInputStream getInputStream() throws IOException {

		final ByteArrayInputStream inputStream = new ByteArrayInputStream(body);

		return new ServletInputStream() {
			@Override
			public int read() throws IOException {
				return inputStream.read();
			}

			@Override
			public boolean isFinished() {
				return false;
			}

			@Override
			public boolean isReady() {
				return false;
			}

			@Override
			public void setReadListener(ReadListener readListener) {
			}
		};
	}

}