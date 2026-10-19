/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.bi.ingestion.config;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public final class Env {

    private final Function<String, String> lookup;

    public Env(Function<String, String> lookup) {
        this.lookup = lookup;
    }

    public static Env system() {
        return new Env(name -> {
            String prop = System.getProperty(name);
            return prop != null ? prop : System.getenv(name);
        });
    }

    public static Env of(Map<String, String> values) {
        return new Env(values::get);
    }

    public String string(String name, String defaultValue) {
        String v = lookup.apply(name);
        return v == null || v.isBlank() ? defaultValue : v.trim();
    }

    public String required(String name) {
        String v = string(name, null);
        if (v == null) {
            throw new IllegalStateException("Required configuration " + name + " is not set");
        }
        return v;
    }

    public int integer(String name, int defaultValue) {
        String v = string(name, null);
        return v == null ? defaultValue : Integer.parseInt(v);
    }

    public long longValue(String name, long defaultValue) {
        String v = string(name, null);
        return v == null ? defaultValue : Long.parseLong(v);
    }

    public double decimal(String name, double defaultValue) {
        String v = string(name, null);
        return v == null ? defaultValue : Double.parseDouble(v);
    }

    public boolean bool(String name, boolean defaultValue) {
        String v = string(name, null);
        return v == null ? defaultValue : Boolean.parseBoolean(v) || "1".equals(v) || "yes".equalsIgnoreCase(v);
    }

    public static Properties prefixed(Map<String, String> environment, String prefix) {
        Properties props = new Properties();
        environment.forEach((k, v) -> {
            if (k.startsWith(prefix) && v != null && !v.isBlank()) {
                props.setProperty(k.substring(prefix.length()).toLowerCase(Locale.ROOT).replace('_', '.'), v);
            }
        });
        return props;
    }
}
