package com.lab.ycsb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

/**
 * YCSB binding for FastAPI Coordinator (Lab 4).
 */
public class FastApiDbClient extends DB {
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    // HTTP клієнт має бути static, щоб перевикористовувати з'єднання (Connection Pooling)
    // інакше YCSB вичерпає порти ОС за секунди.
    private static OkHttpClient httpClient;
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private String coordinatorUrl;
    private boolean debug = false;

    // Константи YCSB
    private static final String DEFAULT_TABLE = "usertable";

    @Override
    public void init() throws DBException {
        // Синхронізація для ініціалізації статичного клієнта
        synchronized (FastApiDbClient.class) {
            if (httpClient == null) {
                httpClient = new OkHttpClient.Builder()
                        .connectTimeout(5, TimeUnit.SECONDS)
                        .readTimeout(30, TimeUnit.SECONDS) // Лаба 4 може мати затримки через реплікацію
                        .writeTimeout(30, TimeUnit.SECONDS)
                        .build();
            }
        }

        Properties props = getProperties();
        this.coordinatorUrl = props.getProperty("coordinator.url", "http://localhost:8000");
        this.debug = Boolean.parseBoolean(props.getProperty("debug", "false"));

        System.out.println(">>> FastApiDbClient INITIALIZED. Target: " + this.coordinatorUrl);

        // Спроба зареєструвати таблицю (один раз на потік, але це не страшно, координатор обробить дублікати)
        // Це критично, бо ваш координатор тримає таблиці в пам'яті.
        registerTable(DEFAULT_TABLE);
    }

    /**
     * Реєструє таблицю, якщо її немає.
     * Відповідає методу register_table у main.py
     */
    private void registerTable(String tableName) {
        String url = this.coordinatorUrl + "/register_table";
        Map<String, String> payload = new HashMap<>();
        payload.put("name", tableName);
        payload.put("partition_key_name", "id"); // YCSB використовує 'id' як ключ за замовчуванням

        try {
            String json = jsonMapper.writeValueAsString(payload);
            RequestBody body = RequestBody.create(json, JSON_MEDIA_TYPE);
            Request request = new Request.Builder().url(url).post(body).build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful() && response.code() != 400) {
                    System.err.println(">>> WARNING: Failed to register table. Code: " + response.code());
                }
            }
        } catch (Exception e) {
            System.err.println(">>> ERROR: Could not verify table registration: " + e.getMessage());
        }
    }

    @Override
    public void cleanup() throws DBException {
        // Нічого не робимо, клієнт статичний
    }

    // --- READ ---
    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        // main.py endpoint: @app.get("/read/{table}/{key}")
        String url = String.format("%s/read/%s/%s", this.coordinatorUrl, table, key);
        Request request = new Request.Builder().url(url).get().build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                if (response.code() == 404) return Status.NOT_FOUND;
                if (debug) System.err.println("READ Error: " + response.code() + " " + response.body().string());
                return Status.ERROR;
            }

            String responseBody = response.body().string();
            JsonNode root = jsonMapper.readTree(responseBody);

            // ВАЖЛИВО: main.py повертає { ..., "response": { ...data... } }
            JsonNode dataNode = root.get("response");

            if (dataNode == null || dataNode.isNull()) {
                return Status.NOT_FOUND; // Ключ є в метаданих, але значення пусте
            }

            // Конвертуємо JSON назад у формат YCSB
            Map<String, Object> dataMap = jsonMapper.convertValue(dataNode, new TypeReference<Map<String, Object>>(){});

            if (fields == null || fields.isEmpty()) {
                // Повертаємо всі поля
                for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                    result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
                }
            } else {
                // Повертаємо тільки запитані поля
                for (String field : fields) {
                    if (dataMap.containsKey(field)) {
                        result.put(field, new StringByteIterator(dataMap.get(field).toString()));
                    }
                }
            }
            return Status.OK;

        } catch (Exception e) {
            if (debug) e.printStackTrace();
            return Status.ERROR;
        }
    }

    // --- SCAN (Не підтримується Key-Value сховищем) ---
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    // --- UPDATE ---
    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        // main.py endpoint: @app.put("/update") -> KeyValue model
        return sendWriteRequest(table, key, values, "/update", "PUT");
    }

    // --- INSERT ---
    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        // main.py endpoint: @app.post("/create") -> KeyValue model
        return sendWriteRequest(table, key, values, "/create", "POST");
    }

    // --- DELETE ---
    @Override
    public Status delete(String table, String key) {
        // main.py endpoint: @app.delete("/delete/{table}/{key}")
        String url = String.format("%s/delete/%s/%s", this.coordinatorUrl, table, key);
        Request request = new Request.Builder().url(url).delete().build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful() || response.code() == 404) {
                return Status.OK;
            }
            return Status.ERROR;
        } catch (Exception e) {
            if (debug) e.printStackTrace();
            return Status.ERROR;
        }
    }

    /**
     * Допоміжний метод для Create/Update
     */
    private Status sendWriteRequest(String table, String key, Map<String, ByteIterator> values, String endpoint, String method) {
        String url = this.coordinatorUrl + endpoint;

        // Конвертація YCSB Map<String, ByteIterator> -> Java Map<String, String>
        Map<String, String> stringValues = new HashMap<>();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            stringValues.put(entry.getKey(), entry.getValue().toString());
        }

        // Формування тіла запиту згідно моделі KeyValue у main.py
        Map<String, Object> payload = new HashMap<>();
        payload.put("table", table);
        payload.put("key", key);
        payload.put("value", stringValues); // Ось тут лежать дані
        // sort_key опускаємо, YCSB його не генерує стандартно

        try {
            String json = jsonMapper.writeValueAsString(payload);
            RequestBody body = RequestBody.create(json, JSON_MEDIA_TYPE);

            Request.Builder builder = new Request.Builder().url(url);
            if (method.equals("POST")) builder.post(body);
            else builder.put(body);

            try (Response response = httpClient.newCall(builder.build()).execute()) {
                if (response.isSuccessful()) {
                    return Status.OK;
                }
                if (debug) {
                    System.err.println("WRITE Error (" + method + "): " + response.code() + " " + response.body().string());
                }
                return Status.ERROR;
            }
        } catch (Exception e) {
            if (debug) e.printStackTrace();
            return Status.ERROR;
        }
    }
}