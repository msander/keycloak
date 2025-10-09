package org.keycloak.valkey.testing;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.keycloak.Config;

/**
 * Minimal {@link Config.Scope} implementation backed by nested {@link Map} structures for tests.
 */
public class MapBackedConfigScope implements Config.Scope {

    private final Map<String, String> values;
    private final Map<String, MapBackedConfigScope> children;
    private final MapBackedConfigScope root;

    private MapBackedConfigScope(Map<String, String> values, Map<String, MapBackedConfigScope> children,
            MapBackedConfigScope root) {
        this.values = values;
        this.children = children;
        this.root = root == null ? this : root;
    }

    public static MapBackedConfigScope from(Map<String, String> values) {
        return new MapBackedConfigScope(new HashMap<>(values), new HashMap<>(), null);
    }

    public MapBackedConfigScope withChild(String name, Map<String, String> values) {
        MapBackedConfigScope child = new MapBackedConfigScope(new HashMap<>(values), new HashMap<>(), root);
        children.put(name, child);
        return child;
    }

    @Override
    public String get(String key) {
        return values.get(key);
    }

    @Override
    public String get(String key, String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }

    @Override
    public String[] getArray(String key) {
        String value = values.get(key);
        if (value == null) {
            return null;
        }
        String[] parts = value.split(",");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
        }
        return parts;
    }

    @Override
    public Integer getInt(String key) {
        return getInt(key, null);
    }

    @Override
    public Integer getInt(String key, Integer defaultValue) {
        String value = values.get(key);
        return value == null ? defaultValue : Integer.valueOf(value);
    }

    @Override
    public Long getLong(String key) {
        return getLong(key, null);
    }

    @Override
    public Long getLong(String key, Long defaultValue) {
        String value = values.get(key);
        return value == null ? defaultValue : Long.valueOf(value);
    }

    @Override
    public Boolean getBoolean(String key) {
        return getBoolean(key, null);
    }

    @Override
    public Boolean getBoolean(String key, Boolean defaultValue) {
        String value = values.get(key);
        return value == null ? defaultValue : Boolean.valueOf(value);
    }

    @Override
    public Config.Scope scope(String... scope) {
        if (scope == null || scope.length == 0) {
            return this;
        }
        MapBackedConfigScope current = this;
        for (String segment : scope) {
            current = current.children.computeIfAbsent(segment, ignored -> new MapBackedConfigScope(new HashMap<>(), new HashMap<>(), root));
        }
        return current;
    }

    @Override
    public Set<String> getPropertyNames() {
        return Collections.unmodifiableSet(values.keySet());
    }

    @Override
    public Config.Scope root() {
        return root;
    }

    public MapBackedConfigScope put(String key, String value) {
        values.put(key, value);
        return this;
    }
}
