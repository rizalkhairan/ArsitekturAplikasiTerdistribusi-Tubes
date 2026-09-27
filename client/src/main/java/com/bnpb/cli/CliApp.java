package com.bnpb.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CliApp {

    private static final String API_URL = "http://localhost:9080";
    private static final Path SESSION_FILE = Paths.get(System.getProperty("user.home"), ".bnpb-cli-session.json");
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private String accessToken;
    private String refreshToken;
    private String clientId;
    private String clientSecret;
    private JsonNode userInfo;
    private boolean autoRefresh = true;
    private final List<TokenRecord> tokenHistory = new ArrayList<>();

    public static void main(String[] args) {
        CliApp app = new CliApp();
        app.loadSession();
        app.run();
    }

    private void run() {
        printBanner();
        printHelp();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("\n\033[1;36mBNPB>\033[0m ");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 2);
            String cmd = parts[0].toLowerCase();
            String arg = parts.length > 1 ? parts[1] : "";

            try {
                switch (cmd) {
                    case "login" -> login(arg);
                    case "refresh" -> refresh();
                    case "autorefresh" -> toggleAutoRefresh(arg);
                    case "whoami" -> whoami();
                    case "tokens" -> showTokens();
                    case "hazards" -> hazards(arg);
                    case "hazard" -> hazard(arg);
                    case "logout" -> logout();
                    case "help" -> printHelp();
                    case "clear" -> clearScreen();
                    case "exit", "quit" -> { logout(); System.out.println("Goodbye!"); return; }
                    default -> System.out.println("\033[1;31mUnknown command: " + cmd + "\033[0m");
                }
            } catch (Exception e) {
                System.out.println("\033[1;31mError: " + e.getMessage() + "\033[0m");
            }
        }
    }

    private void login(String arg) {
        String[] parts = arg.split("\\s+");
        if (parts.length < 2) {
            System.out.println("Usage: login <username> <password>");
            return;
        }
        this.clientId = parts[0];
        this.clientSecret = parts[1];

        System.out.println("\033[1;33mAuthenticating...\033[0m");

        try {
            String url = API_URL + "/auth/login";
            String body = String.format(
                "{\"username\":\"%s\",\"password\":\"%s\"}",
                clientId, clientSecret
            );

            JsonNode response = httpPost(url, body, null);
            if (response.has("status") && "ok".equals(response.get("status").asText())) {
                JsonNode data = response.get("data");
                this.accessToken = data.get("accessToken").asText();
                this.refreshToken = data.has("refreshToken") ? data.get("refreshToken").asText() : null;

                tokenHistory.add(new TokenRecord(accessToken, Instant.now(), "login"));
                System.out.println("\033[1;32mLogin successful!\033[0m");
                System.out.println("  Username: \033[1;36m" + clientId + "\033[0m");
                System.out.println("  Token TTL: \033[1;36m60 seconds\033[0m");
                System.out.println("  Auto-refresh: \033[1;36m" + (autoRefresh ? "ON" : "OFF") + "\033[0m");
                fetchUserInfo();
                saveSession();
            } else {
                System.out.println("\033[1;31mLogin failed: " + response + "\033[0m");
            }
        } catch (Exception e) {
            System.out.println("\033[1;31mLogin error: " + e.getMessage() + "\033[0m");
        }
    }

    private void refresh() {
        if (refreshToken == null) {
            System.out.println("\033[1;31mNo refresh token. Please login first.\033[0m");
            return;
        }

        System.out.println("\033[1;33mRefreshing token...\033[0m");

        try {
            String url = API_URL + "/auth/refresh";
            String body = String.format("{\"refresh_token\":\"%s\"}", refreshToken);

            JsonNode response = httpPost(url, body, null);
            if (response.has("status") && "ok".equals(response.get("status").asText())) {
                JsonNode data = response.get("data");
                this.accessToken = data.get("accessToken").asText();
                if (data.has("refreshToken")) {
                    this.refreshToken = data.get("refreshToken").asText();
                }
                tokenHistory.add(new TokenRecord(accessToken, Instant.now(), "refresh"));
                System.out.println("\033[1;32mToken refreshed!\033[0m");
                saveSession();
            } else {
                System.out.println("\033[1;31mRefresh failed: " + response + "\033[0m");
            }
        } catch (Exception e) {
            System.out.println("\033[1;31mRefresh error: " + e.getMessage() + "\033[0m");
        }
    }

    private void toggleAutoRefresh(String arg) {
        if (arg.isEmpty()) {
            // Toggle
            autoRefresh = !autoRefresh;
        } else {
            // Set explicitly
            autoRefresh = arg.equalsIgnoreCase("true") || arg.equalsIgnoreCase("on") || arg.equalsIgnoreCase("1");
        }
        System.out.println("Auto-refresh: \033[1;36m" + (autoRefresh ? "ON" : "OFF") + "\033[0m");
        saveSession();
    }

    private void whoami() {
        if (accessToken == null) {
            System.out.println("\033[1;31mNot logged in. Use 'login <username> <password>' first.\033[0m");
            return;
        }
        if (userInfo != null) {
            System.out.println("\033[1;36m=== Current User Info ===\033[0m");
            try {
                System.out.println(MAPPER.writeValueAsString(userInfo));
            } catch (Exception e) {
                System.out.println(userInfo);
            }
        } else {
            System.out.println("\033[1;33mFetching user info...\033[0m");
            fetchUserInfo();
        }
    }

    private void showTokens() {
        System.out.println("\033[1;36m=== Token History ===\033[0m");
        if (tokenHistory.isEmpty()) {
            System.out.println("  No tokens received yet.");
            return;
        }
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
        for (int i = 0; i < tokenHistory.size(); i++) {
            TokenRecord t = tokenHistory.get(i);
            System.out.printf("  [%d] %s | %s | %s%n", i + 1, fmt.format(t.time), t.type,
                t.token.substring(0, Math.min(50, t.token.length())) + "...");
        }
        System.out.println("\n  Current access token: " + (accessToken != null ? "\033[1;32mactive\033[0m" : "\033[1;31mnone\033[0m"));
        System.out.println("  Current refresh token: " + (refreshToken != null ? "\033[1;32mactive\033[0m" : "\033[1;31mnone\033[0m"));
        System.out.println("  Auto-refresh: " + (autoRefresh ? "\033[1;32mON\033[0m" : "\033[1;31mOFF\033[0m"));
    }

    private void hazards(String arg) {
        if (accessToken == null) {
            System.out.println("\033[1;31mNot logged in. Use 'login <username> <password>' first.\033[0m");
            return;
        }

        // Auto-refresh if enabled and token is expired
        if (autoRefresh && refreshToken != null) {
            try {
                if (isTokenExpired()) {
                    System.out.println("\033[1;33mToken expired. Auto-refreshing...\033[0m");
                    refresh();
                }
            } catch (Exception e) {
                // Ignore refresh errors here
            }
        }

        String url = API_URL + "/hazards";
        if (!arg.isEmpty()) {
            url += "?" + arg;
        }

        System.out.println("\033[1;33mFetching hazards...\033[0m");
        try {
            JsonNode response = httpGet(url, accessToken);
            try {
                System.out.println(MAPPER.writeValueAsString(response));
            } catch (Exception ex) {
                System.out.println(response);
            }
        } catch (Exception e) {
            System.out.println("\033[1;31mError: " + e.getMessage() + "\033[0m");
        }
    }

    private void hazard(String arg) {
        if (accessToken == null) {
            System.out.println("\033[1;31mNot logged in. Use 'login <username> <password>' first.\033[0m");
            return;
        }
        if (arg.isEmpty()) {
            System.out.println("Usage: hazard <id>");
            return;
        }

        // Auto-refresh if enabled and token is expired
        if (autoRefresh && refreshToken != null) {
            try {
                if (isTokenExpired()) {
                    System.out.println("\033[1;33mToken expired. Auto-refreshing...\033[0m");
                    refresh();
                }
            } catch (Exception e) {
                // Ignore refresh errors here
            }
        }

        String url = API_URL + "/hazards/" + arg;
        System.out.println("\033[1;33mFetching hazard " + arg + "...\033[0m");
        try {
            JsonNode response = httpGet(url, accessToken);
            try {
                System.out.println(MAPPER.writeValueAsString(response));
            } catch (Exception ex) {
                System.out.println(response);
            }
        } catch (Exception e) {
            System.out.println("\033[1;31mError: " + e.getMessage() + "\033[0m");
        }
    }

    private boolean isTokenExpired() {
        if (accessToken == null) return true;
        try {
            String[] parts = accessToken.split("\\.");
            if (parts.length != 3) return true;
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            JsonNode claims = MAPPER.readTree(payload);
            if (claims.has("exp")) {
                long exp = claims.get("exp").asLong();
                return System.currentTimeMillis() / 1000 >= exp;
            }
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    private void logout() {
        accessToken = null;
        refreshToken = null;
        userInfo = null;
        tokenHistory.clear();
        saveSession();
        System.out.println("\033[1;33mLogged out.\033[0m");
    }

    private void fetchUserInfo() {
        // User info is now fetched from the access token claims
        if (accessToken != null) {
            try {
                String[] parts = accessToken.split("\\.");
                if (parts.length == 3) {
                    String payload = new String(java.util.Base64.getUrlDecoder().decode(parts[1]));
                    userInfo = new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload);
                }
            } catch (Exception e) {
                // Ignore - token may not be JWT
            }
        }
    }

    private JsonNode httpGet(String urlStr, String token) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(10000);

        int status = conn.getResponseCode();
        InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        String body = new String(is.readAllBytes());

        if (status == 401) {
            throw new RuntimeException("\033[1;31m401 Unauthorized\033[0m — Token expired or invalid. Try 'refresh' or 'login' again.");
        }
        if (status == 403) {
            throw new RuntimeException("\033[1;31m403 Forbidden\033[0m — Insufficient scope.");
        }
        if (status >= 400) {
            throw new RuntimeException("\033[1;31mHTTP " + status + "\033[0m — " + body);
        }
        return MAPPER.readTree(body);
    }

    private JsonNode httpPost(String urlStr, String body, String token) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        if (token != null) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }
        conn.setDoOutput(true);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(10000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes());
        }

        int status = conn.getResponseCode();
        InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        String responseBody = new String(is.readAllBytes());
        return MAPPER.readTree(responseBody);
    }

    private void saveSession() {
        try {
            Map<String, Object> session = new LinkedHashMap<>();
            session.put("accessToken", accessToken);
            session.put("refreshToken", refreshToken);
            session.put("clientId", clientId);
            session.put("userInfo", userInfo);
            session.put("autoRefresh", autoRefresh);
            session.put("tokenHistory", tokenHistory);
            try {
                Files.writeString(SESSION_FILE, MAPPER.writeValueAsString(session));
            } catch (Exception e) {
                // Ignore save errors
            }
        } catch (Exception e) {
            // Ignore save errors
        }
    }

    private void loadSession() {
        try {
            if (Files.exists(SESSION_FILE)) {
                JsonNode session = MAPPER.readTree(Files.readString(SESSION_FILE));
                this.accessToken = session.has("accessToken") ? session.get("accessToken").asText() : null;
                this.refreshToken = session.has("refreshToken") ? session.get("refreshToken").asText() : null;
                this.clientId = session.has("clientId") ? session.get("clientId").asText() : null;
                this.userInfo = session.has("userInfo") ? session.get("userInfo") : null;
                this.autoRefresh = session.has("autoRefresh") ? session.get("autoRefresh").asBoolean() : true;
                if (session.has("tokenHistory")) {
                    for (JsonNode t : session.get("tokenHistory")) {
                        tokenHistory.add(new TokenRecord(
                            t.get("token").asText(),
                            Instant.parse(t.get("time").asText()),
                            t.get("type").asText()
                        ));
                    }
                }
            }
        } catch (Exception e) {
            // Ignore load errors
        }
    }

    private void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    private void printBanner() {
        System.out.println("\033[1;36m");
        System.out.println("  ____  _   _ ____  ____  ");
        System.out.println(" | __ )| \\ | |  _ \\| __ ) ");
        System.out.println(" |  _ \\|  \\| | |_) |  _ \\ ");
        System.out.println(" | |_) | |\\  |  __/| |_) |");
        System.out.println(" |____/|_| \\_|_|   |____/ ");
        System.out.println("   BNPB Disaster Coordination CLI");
        System.out.println("\033[0m");
    }

    private void printHelp() {
        System.out.println("\033[1;36m=== Available Commands ===\033[0m");
        System.out.println("  \033[1;33mlogin <username> <password>\033[0m       — Authenticate and get access token");
        System.out.println("  \033[1;33mrefresh\033[0m                         — Refresh access token manually");
        System.out.println("  \033[1;33mautorefresh [on|off]\033[0m             — Toggle auto-refresh (no arg = toggle)");
        System.out.println("  \033[1;33mwhoami\033[0m                           — Show current user info");
        System.out.println("  \033[1;33mtokens\033[0m                           — Show all received tokens");
        System.out.println("  \033[1;33mhazards [query]\033[0m                   — List hazard events (e.g. ?source=BMKG&limit=10)");
        System.out.println("  \033[1;33mhazard <id>\033[0m                       — Get single hazard event by ID");
        System.out.println("  \033[1;33mlogout\033[0m                           — Clear session and logout");
        System.out.println("  \033[1;33mclear\033[0m                            — Clear screen");
        System.out.println("  \033[1;33mhelp\033[0m                              — Show this help");
        System.out.println("  \033[1;33mexit\033[0m                              — Exit CLI");
        System.out.println();
        System.out.println("\033[1;36m=== Quick Start ===\033[0m");
        System.out.println("  1. login media-user media-pass-2026    — Login as media (summary scope)");
        System.out.println("  2. hazards                              — List all hazards");
        System.out.println("  3. hazards ?source=BMKG&limit=5         — Filter by source");
        System.out.println("  4. autorefresh off                      — Disable auto-refresh");
        System.out.println("        \033[1;33mNote:\033[0m Token expires in 60 seconds. Auto-refresh is ON by default.");
    }

    private record TokenRecord(String token, Instant time, String type) {}
}
