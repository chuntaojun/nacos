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

import java.util.Objects;

/**
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public final class HashUtils {

	/**
	 * Perform a hash calculation based on the key value to find a responsible person from {@link T[]}
	 *
	 * @param array T[]
	 * @param key key
	 * @param <T> type
	 * @return T
	 */
	public static <T> T mapper(T[] array, Object key) {
		int hash = Math.abs(Objects.hashCode(key));
		int index = Math.abs(hash % Integer.MAX_VALUE) % array.length;
		return array[index];
	}

}
