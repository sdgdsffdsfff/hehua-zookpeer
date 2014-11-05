/**
 * 
 */
package com.hehua.framework.config;

import java.util.Map;

/**
 * @author zhihua
 *
 */
public interface ConfigManager {

    public Map<String, String> getAll();

    public String getString(String key);

    public void setString(String key, String value);
}
