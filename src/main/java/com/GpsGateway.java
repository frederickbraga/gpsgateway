package com;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.*;
import smgo.Comum.Cabecalho;
import smgo.Todos;
import smgo.Todos.Pacote;
import smgo.Todos.TipoPacote;
import smgo.posicao.PosicionamentoOuterClass.MensagemPosicionamento;
import smgo.posicao.PosicionamentoOuterClass.Posicionamento;
import smgo.posicao.PosicionamentoOuterClass.Posicionamento.QualidadeGPS;
import smgo.posicao.PosicionamentoOuterClass.TipoMensagemPosicionamento;

//import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class GpsGateway {

    /**
     * Host e porta do servidor MQTT. Será fornecido pela Cittati.
     */
    private static final String URL = "ssl://mqtt.fornecedor.com.br:5883";

    /**
     * Identificador <b>único</b> para um cliente MQTT perante o servidor.
     */
    private static final String CLIENT_ID = "7585";

    /**
     * Username de autenticação para conectar-se no servidor MQTT. Será fornecido pela Cittati.
     */
    private static final String USERNAME = "user";

    /**
     * Senha de autenticação para conectar-se no servidor MQTT. Será fornecido pela Cittati.
     */
    private static final String PASSWORD = "senha";

    /**
     * Nome da fila/tópico MQTT onde as mensagens protobuf de posicionamento serão publicadas pelo cliente.
     * Será fornecido pela Cittati.
     */
    private static final String QUEUE_NAME = "avl.empresa.posicionamento";

    private static int id = 0;
    private static double latitude = 0;
    private static double longitude = 0;
    private static long uniqueid = 0;
    private static int altitude = 0;
    private static int speed = 0;
    private static long devicetime = 0;
    private static boolean ignition = false;

    public static void main(String[] args) throws MqttException, InterruptedException {

        // --------------------------------------------------------------------
        // Cria/configura cliente MQTT

        MqttClient sampleClient = new MqttClient(URL, CLIENT_ID, new MemoryPersistence());  // Pode trocar por MqttDefaultFilePersistence
        MqttConnectOptions options = new MqttConnectOptions();

        options.setUserName(USERNAME);
        options.setPassword(PASSWORD.toCharArray());

        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setMaxReconnectDelay(120);

        // --------------------------------------------------------------------
        // Conecta no servidor MQTT

        sampleClient.connect(options);

        // --------------------------------------------------------------------
        // Exemplo 1: publica uma mensagem (protobuf) de posicionamento

        String url = "jdbc:mysql://localhost:3306/tracar?useSSL=false";
        String user = "root";
        String password = "Muda123*";

        String query = "SELECT P.id, P.devicetime, P.latitude, P.longitude, P.altitude, P.speed, D.uniqueid, P.attributes FROM tracar.tc_positions P JOIN tracar.tc_devices D ON D.ID = P.deviceid WHERE sent = 0 ORDER BY P.devicetime;";
        int pacote = 0;
        while (true) {
            try (Connection con = DriverManager.getConnection(url, user, password);

                 Statement st = con.createStatement();
                 ResultSet rs = st.executeQuery(query)) {

                System.out.println("Processando novos registros...");

                while (rs.next()) {

                    pacote++;

                    latitude = rs.getDouble("latitude");
                    longitude = rs.getDouble("longitude");
                    uniqueid = Long.parseLong(rs.getString("uniqueid"));
                    altitude = rs.getInt("altitude");
                    speed = rs.getInt("speed");
                    id = rs.getInt("id");
                    devicetime = LocalDateTime.parse(
                                    rs.getString("devicetime")
                                            .replace(" ", "T")
                            ).minusHours(3)
                            .atZone(
                                    ZoneId.of("America/Sao_Paulo")
                            )
                            .toInstant()
                            .toEpochMilli();

                    String json = rs.getString("attributes");
                    try {
                        JSONObject jsonObject = new JSONObject(json);
                        ignition = jsonObject.getBoolean("ignition");
                    }
                    catch (JSONException jsonException){
                        ignition = true;
                    }

                    System.out.println("Processando registro: " + rs.getString("id") + " Pacote: " + pacote);

                    boolean sent;
                    try {
                        Todos.Pacote pacotePosicao = criaPacotePosicionamento();
                        sampleClient.publish(QUEUE_NAME, pacotePosicao.toByteArray(), 1, true);
                        sent = true;
                    }
                    catch ( Exception e){
                        sent = false;
                    }

                    if (sent) {
                        String update = "UPDATE tc_positions SET sent = 1 WHERE id = " + rs.getInt("id") + ";";
                        Statement st1 = con.createStatement();
                        st1.executeUpdate(update);
                    }

                    Thread.sleep(100);
                }

            } catch (SQLException sqlException) {

                System.out.println(sqlException.getMessage());
            }
            Thread.sleep(10000);
        }

        // QoS 1 é o recomendado para Produção. Para testes, pode usar 0.
        // 'retained' em Produção é recomendado true, mas para testes pode usar false.

        // --------------------------------------------------------------------
        // Exemplo 2: publica uma mensagem (protobuf) de ping
        //
        // Recomenda-se enviar um pacote de ping para o servidor se não houver dado de posicionamento disponível (por
        // algum problema no GPS ou área de sombra, por exemplo). Com isso, o servidor sabe que o equipamento está "vivo".

        //Todos.Pacote pacotePing = criaPacotePing();
        //sampleClient.publish(QUEUE_NAME, pacotePing.toByteArray(), 1, true);

    }

    private static Todos.Pacote criaPacotePosicionamento() {
        return Pacote.newBuilder()
                .setCabecalho(criaCabecalho(id))
                .setTipo(TipoPacote.POSICIONAMENTO) // Identifica o conteúdo/tipo do pacote sendo enviado
                .setPosicionamentos(criaMensagemPosicionamento())
                .build();
    }
    private static Todos.Pacote criaPacotePing() {
        // Um pacote de ping não tem "conteúdo", consistindo apenas em cabeçalho e tipo.
        return Pacote.newBuilder()
                .setCabecalho(criaCabecalho(id))
                .setTipo(TipoPacote.PING) // Identifica o conteúdo/tipo do pacote sendo enviado
                .build();
    }

    private static Cabecalho.Builder criaCabecalho(int sequencial) {
        return Cabecalho.newBuilder()
                .setDataHora(System.currentTimeMillis() / 1000) // Epoch TS, em segundos
                .setNumeroSerial(uniqueid)                            // Se o serial do equipamento for numérico; se for alfanumérico (string), usar setIdentificadorEquipamento()
                .setVersaoProtocolo(1)                          // Pode deixar sempre 1
                .setFirmware(2)                                 // Se for possível preencher, é a versão (numérica) do firmware/equipamento enviando o pacote
                .setNumeroSequencial(sequencial);               // Identificador único para o pacote
    }

    private static MensagemPosicionamento.Builder criaMensagemPosicionamento() {
        return MensagemPosicionamento.newBuilder()
                .addPosicoes(Posicionamento.newBuilder()
                        .setDataHora(devicetime / 1000)    // Epoch TS, em segundos
                        .setQualidade(QualidadeGPS.POSICIONAMENTO_3D)
                        .setLatitude(latitude)                                  // Em graus
                        .setLongitude(longitude)                                 // Em graus
                        .setAltura(altitude)                                     // Em metros
                        .setAngulo(0)                                      // Em graus
                        .setVelocidade(speed)                                 // Em km/h
                        .setIgnicao(ignition)                                  // Se não estiver disponível, enviar 'false'
                        .setTipo(TipoMensagemPosicionamento.POSICIONAMENTO) // Quando for uma entrada em algum terminal, por exemplo
                        .setIdentificadorPonto(0)                         // Apenas para equipamentos que suportam carga de dados
                );
    }
}
