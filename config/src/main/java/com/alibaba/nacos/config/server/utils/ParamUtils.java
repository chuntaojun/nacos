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

package com.alibaba.nacos.config.server.utils;

import java.util.Map;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.Constants;

/**
 * Parameter validity check util.
 *
 * @author Nacos
 */
public class ParamUtils {
    
    private static char[] validChars = new char[] {'_', '-', '.', ':'};
    
    private static final int TAG_MAX_LEN = 16;
    
    private static final String NAMESPACE_PUBLIC_KEY = "public";
    
    private static final String NAMESPACE_NULL_KEY = "null";
    
    private static final int NAMESPACE_MAX_LEN = 128;
    
    
    /**
     * Whitelist checks that valid parameters can only contain letters, Numbers, and characters in validChars, and
     * cannot be empty.
     */
    public static boolean isValid(String param) {
        if (param == null) {
            return false;
        }
        int length = param.length();
        for (int i = 0; i < length; i++) {
            char ch = param.charAt(i);
            if (!Character.isLetterOrDigit(ch) && !isValidChar(ch)) {
                return false;
            }
        }
        return true;
    }
    
    private static boolean isValidChar(char ch) {
        for (char c : validChars) {
            if (c == ch) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check the parameter.
     */
    public static void checkParam(String dataId, String group, String datumId, String content) throws NacosException {
        if (StringUtils.isBlank(dataId) || !isValid(dataId.trim())) {
            throw new NacosException(NacosException.INVALID_PARAM, "invalid dataId : " + dataId);
        } else if (StringUtils.isBlank(group) || !isValid(group)) {
            throw new NacosException(NacosException.INVALID_PARAM, "invalid group : " + group);
        } else if (StringUtils.isBlank(datumId) || !isValid(datumId)) {
            throw new NacosException(NacosException.INVALID_PARAM, "invalid datumId : " + datumId);
        } else if (StringUtils.isBlank(content)) {
            throw new NacosException(NacosException.INVALID_PARAM, "content is blank : " + content);
        } else if (content.length() > PropertyUtil.getMaxContent()) {
            throw new NacosException(NacosException.INVALID_PARAM,
                    "invalid content, over " + PropertyUtil.getMaxContent());
        }
    }
    
    /**
     * Check the tag.
     */
    public static void checkTag(String tag) {
        if (StringUtils.isNotBlank(tag)) {
            if (!isValid(tag.trim())) {
                throw new IllegalArgumentException("invalid tag : " + tag);
            }
            if (tag.length() > TAG_MAX_LEN) {
                throw new IllegalArgumentException("too long tag, over 16");
            }
        }
    }
    
    /**
     * Check the config info.
     */
    public static void checkConfigInfo(Map<String, Object> configAdvanceInfo) throws NacosException {
        for (Map.Entry<String, Object> configAdvanceInfoTmp : configAdvanceInfo.entrySet()) {
            if ("config_tags".equals(configAdvanceInfoTmp.getKey())) {
                if (configAdvanceInfoTmp.getValue() != null) {
                    String[] tagArr = ((String) configAdvanceInfoTmp.getValue()).split(",");
                    if (tagArr.length > 5) {
                        throw new NacosException(NacosException.INVALID_PARAM, "too much config_tags, over 5");
                    }
                    for (String tag : tagArr) {
                        if (tag.length() > 64) {
                            throw new NacosException(NacosException.INVALID_PARAM, "too long tag, over 64");
                        }
                    }
                }
            } else if ("desc".equals(configAdvanceInfoTmp.getKey())) {
                if (configAdvanceInfoTmp.getValue() != null
                        && ((String) configAdvanceInfoTmp.getValue()).length() > 128) {
                    throw new NacosException(NacosException.INVALID_PARAM, "too long desc, over 128");
                }
            } else if ("use".equals(configAdvanceInfoTmp.getKey())) {
                if (configAdvanceInfoTmp.getValue() != null
                        && ((String) configAdvanceInfoTmp.getValue()).length() > 32) {
                    throw new NacosException(NacosException.INVALID_PARAM, "too long use, over 32");
                }
            } else if ("effect".equals(configAdvanceInfoTmp.getKey())) {
                if (configAdvanceInfoTmp.getValue() != null
                        && ((String) configAdvanceInfoTmp.getValue()).length() > 32) {
                    throw new NacosException(NacosException.INVALID_PARAM, "too long effect, over 32");
                }
            } else if ("type".equals(configAdvanceInfoTmp.getKey())) {
                if (configAdvanceInfoTmp.getValue() != null
                        && ((String) configAdvanceInfoTmp.getValue()).length() > 32) {
                    throw new NacosException(NacosException.INVALID_PARAM, "too long type, over 32");
                }
            } else if ("schema".equals(configAdvanceInfoTmp.getKey())) {
                if (configAdvanceInfoTmp.getValue() != null
                        && ((String) configAdvanceInfoTmp.getValue()).length() > 32768) {
                    throw new NacosException(NacosException.INVALID_PARAM, "too long schema, over 32768");
                }
            } else {
                throw new NacosException(NacosException.INVALID_PARAM, "invalid param");
            }
        }
    }
    
    public static int checkInteger(Integer arg, int defaultVal) {
        if (null == arg) {
            return defaultVal;
        }
        return arg;
    }
    
    /**
     * Check the namespace.
     */
    public static void checkNamespace(String namespace) {
        if (StringUtils.isNotBlank(namespace)) {
            if (!ParamUtils.isValid(namespace.trim())) {
                throw new IllegalArgumentException("invalid namespace");
            }
            if (namespace.length() > NAMESPACE_MAX_LEN) {
                throw new IllegalArgumentException("too long tag, over 128");
            }
        }
    }
    
    public static String processDataID(final String dataID) {
        return dataID.trim();
    }
    
    public static String processGroupID(final String groupID) {
        final String tmpGroupID = groupID.trim();
        return StringUtils.isBlank(tmpGroupID) ? Constants.DEFAULT_GROUP : tmpGroupID;
    }
    
    /**
     * Treat the namespace(tenant) parameters with values of "public" and "null" as an empty string.
     * @param namespace namespace(tenant) id
     * @return java.lang.String A namespace(tenant) string processed
     */
    public static String processNamespace(final String namespace) {
        ParamUtils.checkNamespace(namespace);
        if (StringUtils.isBlank(namespace) || NAMESPACE_PUBLIC_KEY.equalsIgnoreCase(namespace) || NAMESPACE_NULL_KEY
                .equalsIgnoreCase(namespace)) {
            return "";
        }
        return namespace.trim();
    }
    
}
