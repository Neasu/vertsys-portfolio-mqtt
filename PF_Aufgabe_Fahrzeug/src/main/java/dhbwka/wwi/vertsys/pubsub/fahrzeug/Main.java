/*
 * Copyright © 2018 Dennis Schulmeister-Zimolong
 * 
 * E-Mail: dhbw@windows3.de
 * Webseite: https://www.wpvs.de/
 * 
 * Dieser Quellcode ist lizenziert unter einer
 * Creative Commons Namensnennung 4.0 International Lizenz.
 */
package dhbwka.wwi.vertsys.pubsub.fahrzeug;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 * Hauptklasse unseres kleinen Progrämmchens.
 *
 * Mit etwas Google-Maps-Erfahrung lassen sich relativ einfach eigene
 * Wegstrecken definieren. Man muss nur Rechtsklick auf einen Punkt machen und
 * "Was ist hier?" anklicken, um die Koordinaten zu sehen. Allerdings speichert
 * Goolge Maps eine Nachkommastelle mehr, als das ITN-Format erlaubt. :-)
 */
public class Main {

    MqttClient client;

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        try {
            main.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run() throws Exception {
        // Fahrzeug-ID abfragen
        String vehicleId = Utils.askInput("Beliebige Fahrzeug-ID", "postauto");

        // Zu fahrende Strecke abfragen
        File workdir = new File("./waypoints");
        String[] waypointFiles = workdir.list((File dir, String name) -> {
            return name.toLowerCase().endsWith(".itn");
        });

        System.out.println();
        System.out.println("Aktuelles Verzeichnis: " + workdir.getCanonicalPath());
        System.out.println();
        System.out.println("Verfügbare Wegstrecken");
        System.out.println();

        for (int i = 0; i < waypointFiles.length; i++) {
            System.out.println("  [" + i + "] " + waypointFiles[i]);
        }

        System.out.println();
        int index = Integer.parseInt(Utils.askInput("Zu fahrende Strecke", "0"));

        // TODO: Methode parseItnFile() unten ausprogrammieren
        List<WGS84> waypoints = parseItnFile(new File(workdir, waypointFiles[index]));

        // Adresse des MQTT-Brokers abfragen
        String mqttAddress = Utils.askInput("MQTT-Broker", Utils.MQTT_BROKER_ADDRESS);

        // TODO: Sicherstellen, dass bei einem Verbindungsabbruch eine sog.
        // LastWill-Nachricht gesendet wird, die auf den Verbindungsabbruch
        // hinweist. Die Nachricht soll eine "StatusMessage" sein, bei der das
        // Feld "type" auf "StatusType.CONNECTION_LOST" gesetzt ist.
        //
        // Die Nachricht muss dem MqttConnectOptions-Objekt übergeben werden
        // und soll an das Topic Utils.MQTT_TOPIC_NAME gesendet werden.
        StatusMessage lastWill = new StatusMessage();
        lastWill.vehicleId = vehicleId;
        lastWill.type = StatusType.CONNECTION_LOST;
        lastWill.message = "Verbindung verloren";

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setWill(Utils.MQTT_TOPIC_NAME, lastWill.toJson(), 2, false);

        // TODO: Verbindung zum MQTT-Broker herstellen.
        client = new MqttClient(mqttAddress, vehicleId);
        client.connect(options);

        // TODO: Statusmeldung mit "type" = "StatusType.VEHICLE_READY" senden.
        // Die Nachricht soll soll an das Topic Utils.MQTT_TOPIC_NAME gesendet
        // werden.
        StatusMessage readyStatusMsg = new StatusMessage();
        readyStatusMsg.vehicleId = vehicleId;
        readyStatusMsg.type = StatusType.VEHICLE_READY;
        readyStatusMsg.message = "Fahrzeug bereit";

        MqttMessage readyMsg = new MqttMessage();
        readyMsg.setQos(2);
        readyMsg.setPayload(readyStatusMsg.toJson());

        client.publish(Utils.MQTT_TOPIC_NAME, readyMsg);

        // TODO: Thread starten, der jede Sekunde die aktuellen Sensorwerte
        // des Fahrzeugs ermittelt und verschickt. Die Sensordaten sollen
        // an das Topic Utils.MQTT_TOPIC_NAME + "/" + vehicleId gesendet werden.
        Vehicle vehicle = new Vehicle(vehicleId, waypoints);
        vehicle.startVehicle();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    send(Utils.MQTT_TOPIC_NAME + "/" + vehicleId, vehicle.getSensorData());
                } catch (Exception ex) {
                    Utils.logException(ex);
                }
            }
        };

        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(task, 0, 1000);

        // Warten, bis das Programm beendet werden soll
        Utils.fromKeyboard.readLine();

        vehicle.stopVehicle();

        // TODO: Oben vorbereitete LastWill-Nachricht hier manuell versenden,
        // da sie bei einem regulären Verbindungsende nicht automatisch
        // verschickt wird.
        //
        // Anschließend die Verbindung trennen und den oben gestarteten Thread
        // beenden, falls es kein Daemon-Thread ist.
        
        MqttMessage mqttMessage = new MqttMessage(lastWill.toJson());
        mqttMessage.setQos(2);
        client.publish(Utils.MQTT_TOPIC_NAME, mqttMessage);
        client.disconnect();
    }

    /**
     * Öffnet die in "filename" übergebene ITN-Datei und extrahiert daraus die
     * Koordinaten für die Wegstrecke des Fahrzeugs. Das Dateiformat ist ganz
     * simpel:
     *
     * <pre>
     * 0845453|4902352|Point 1 |0|
     * 0848501|4900249|Point 2 |0|
     * 0849295|4899460|Point 3 |0|
     * 0849796|4897723|Point 4 |0|
     * </pre>
     *
     * Jede Zeile enthält einen Wegpunkt. Die Datenfelder einer Zeile werden
     * durch | getrennt. Das erste Feld ist die "Longitude", das zweite Feld die
     * "Latitude". Die Zahlen müssen durch 100_000.0 geteilt werden.
     *
     * @param file ITN-Datei
     * @return Liste mit Koordinaten
     * @throws java.io.IOException
     */
    public static List<WGS84> parseItnFile(File file) throws IOException {
        List<WGS84> waypoints = new ArrayList<>();

        BufferedReader fromFile = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line;

        while ((line = fromFile.readLine()) != null) {
            double latitude, longitude;
            String[] fields = line.split("\\|");

            if (fields.length < 2) {
                continue;
            }

            try {
                longitude = Integer.parseInt(fields[0]) / 100_000.0;
                latitude = Integer.parseInt(fields[1]) / 100_000.0;
            } catch (NumberFormatException ex) {
                Utils.logException(ex);
                continue;
            }

            WGS84 waypoint = new WGS84(latitude, longitude);
            waypoints.add(waypoint);
        }

        return waypoints;
    }

    private void send(String topic, SensorMessage sensorMessage) throws MqttException {
        if (topic != null && sensorMessage != null && client != null) {
            byte[] json = sensorMessage.toJson();
            System.out.println("→ " + new String(json, StandardCharsets.UTF_8));

            MqttMessage mqttMessage = new MqttMessage(json);
            mqttMessage.setQos(0);
            client.publish(topic, mqttMessage);
        }
    }

}
