/*
 * AQUAMONITOR - Unite embarquee ESP32 (version Wokwi)
 * ---------------------------------------------------
 * Surveillance du niveau et de la qualite physico-chimique de l'eau
 * d'une citerne communautaire de Goma.
 *
 * Capteurs simules dans Wokwi :
 *   - HC-SR04        : niveau d'eau (distance au-dessus de la surface)
 *   - DS18B20        : temperature
 *   - Potentiometre 1: pH          (GPIO 34)
 *   - Potentiometre 2: turbidite   (GPIO 35)
 *   - Potentiometre 3: conductivite(GPIO 32)
 *
 * Principe :
 *   1. lecture + validation des mesures ;
 *   2. publication MQTT vers le serveur AQUAMONITOR ;
 *   3. attente d'un ACCUSE DE RECEPTION applicatif sur
 *      aquamonitor/{TYPE}/{CODE}/ack (statut OK, DUPLICATE ou REJECTED) ;
 *   4. sans accuse (coupure reseau, broker indisponible) : stockage
 *      local dans une file BORNEE, puis resynchronisation au
 *      retablissement de la connexion.
 *
 * Fidelite du transport :
 *   PubSubClient ne publie qu'en QoS 0. La fiabilite ne vient donc pas du
 *   QoS MQTT mais de l'accuse applicatif : une mesure n'est retiree de la
 *   file locale qu'apres un ack OK / DUPLICATE / REJECTED. Un ack perdu
 *   provoque un renvoi, que le serveur ignore comme doublon (idempotence).
 *
 * Memoire :
 *   LittleFS par defaut (Wokwi). Pour du materiel reel avec carte SD,
 *   passer USE_SD_CARD a 1 et definir PIN_SD_CS.
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <time.h>

// ------------------------------------------------------------------
// Configuration du stockage (doit preceder les #include conditionnels)
// ------------------------------------------------------------------
#define USE_SD_CARD     0                  // 1 = carte SD (materiel reel), 0 = LittleFS
#define PIN_SD_CS       13                 // uniquement si USE_SD_CARD = 1

#if USE_SD_CARD
#include <SD.h>
static fs::FS& stockage = SD;
#else
#include <LittleFS.h>
static fs::FS& stockage = LittleFS;
#endif

// ------------------------------------------------------------------
// Configuration : a adapter
// ------------------------------------------------------------------
#define WIFI_SSID       "Wokwi-GUEST"
#define WIFI_PASSWORD   ""
#define WIFI_CHANNEL    6

// Wokwi for VS Code : le broker embarque du backend est joignable via
// "host.wokwi.internal". Pour l'editeur en ligne (wokwi.com), utiliser un
// broker public, par exemple "broker.hivemq.com" (voir README.md).
#define MQTT_HOST       "host.wokwi.internal"
#define MQTT_PORT       1883
// Identifiants du broker authentifie (Mosquitto en production).
// Laisser vides avec le broker embarque ou un broker public.
#define MQTT_USER       ""
#define MQTT_PASSWORD   ""

#define DEVICE_ID       "ESP32-CIT-001"
#define DEVICE_TYPE     "CITERNE"          // CITERNE ou CAMION
#define DEVICE_CODE     "CIT-001"          // code citerne / immatriculation camion

#define INTERVALLE_MS   5000               // periode d'acquisition
#define HAUTEUR_CITERNE_CM 200.0           // hauteur utile de la citerne

#define ACK_TIMEOUT_MS  2500               // attente maximale d'un accuse
#define MAX_QUEUE       300                // taille maximale de la file locale
#define RESYNC_LOT      8                  // mesures rejouees par cycle
#define RESYNC_PERIODE_MS 10000            // periode de reprise de la file

// ------------------------------------------------------------------
// Brochage
// ------------------------------------------------------------------
#define PIN_TRIG        5
#define PIN_ECHO        18
#define PIN_DS18B20     4
#define PIN_PH          34
#define PIN_TURBIDITE   35
#define PIN_CONDUCTIVITE 32

// ------------------------------------------------------------------
// Objets
// ------------------------------------------------------------------
WiFiClient reseau;
PubSubClient mqtt(reseau);
OneWire oneWire(PIN_DS18B20);
DallasTemperature sondes(&oneWire);

const char* TOPIC_MESURES = "aquamonitor/" DEVICE_TYPE "/" DEVICE_CODE "/mesures";
const char* TOPIC_STATUT  = "aquamonitor/" DEVICE_TYPE "/" DEVICE_CODE "/statut";
const char* TOPIC_ACK     = "aquamonitor/" DEVICE_TYPE "/" DEVICE_CODE "/ack";

const char* FICHIER_FILE = "/queue.ndjson";
const char* FICHIER_TMP  = "/queue.tmp";

unsigned long derniereMesure = 0;
unsigned long derniereResync = 0;
uint32_t compteurEnvois = 0;
uint32_t compteurFiles = 0;

// Accuse de reception recu (callback MQTT)
bool ackRecu = false;
String ackHorodatage;
String ackStatut;
String ackRaison;

// ------------------------------------------------------------------
// Utilitaires
// ------------------------------------------------------------------
float mediane(float* valeurs, int n) {
  for (int i = 0; i < n - 1; i++) {
    for (int j = i + 1; j < n; j++) {
      if (valeurs[j] < valeurs[i]) {
        float t = valeurs[i];
        valeurs[i] = valeurs[j];
        valeurs[j] = t;
      }
    }
  }
  return (n % 2 == 1) ? valeurs[n / 2] : (valeurs[n / 2 - 1] + valeurs[n / 2]) / 2.0;
}

time_t heureBase = 0;
unsigned long millisBase = 0;

// L'horloge simulee de Wokwi peut rester figee apres la synchronisation NTP.
// On memorise l'instant NTP puis on ajoute le temps ecoule : chaque mesure
// obtient ainsi un horodatage distinct (sinon le serveur la voit comme un
// doublon et ne l'enregistre pas).
void synchroniserHeure() {
  time_t t = time(nullptr);
  if (t > 1700000000) { // apres novembre 2023
    heureBase = t;
    millisBase = millis();
  }
}

bool heureValide() {
  return heureBase > 0;
}

String horodatageIso() {
  time_t maintenant = heureBase + (time_t)((millis() - millisBase) / 1000);
  struct tm* utc = gmtime(&maintenant);
  char tampon[25];
  strftime(tampon, sizeof(tampon), "%Y-%m-%dT%H:%M:%SZ", utc);
  return String(tampon);
}

// ------------------------------------------------------------------
// Capteurs
// ------------------------------------------------------------------
float lireDistanceCm() {
  float mesures[5];
  for (int i = 0; i < 5; i++) {
    digitalWrite(PIN_TRIG, LOW);
    delayMicroseconds(2);
    digitalWrite(PIN_TRIG, HIGH);
    delayMicroseconds(10);
    digitalWrite(PIN_TRIG, LOW);

    long duree = pulseIn(PIN_ECHO, HIGH, 30000);
    mesures[i] = (duree == 0) ? -1.0 : (duree * 0.0343) / 2.0;
    delay(30);
  }

  float valides[5];
  int n = 0;
  for (int i = 0; i < 5; i++) {
    if (mesures[i] >= 2.0 && mesures[i] <= 400.0) valides[n++] = mesures[i];
  }
  if (n == 0) return -1.0;
  return mediane(valides, n);
}

float lirePh() {
  long somme = 0;
  for (int i = 0; i < 10; i++) {
    somme += analogRead(PIN_PH);
    delay(5);
  }
  float tension = (somme / 10.0) / 4095.0;   // 0..1
  // Potentiometre : 0 V -> pH 0, 3,3 V -> pH 14
  return tension * 14.0;
}

float lireTurbidite() {
  long somme = 0;
  for (int i = 0; i < 10; i++) {
    somme += analogRead(PIN_TURBIDITE);
    delay(5);
  }
  float tension = (somme / 10.0) / 4095.0;
  // Potentiometre a fond a droite = eau claire (0 NTU)
  return (1.0 - tension) * 50.0;
}

float lireConductivite() {
  long somme = 0;
  for (int i = 0; i < 10; i++) {
    somme += analogRead(PIN_CONDUCTIVITE);
    delay(5);
  }
  float tension = (somme / 10.0) / 4095.0;
  return tension * 2000.0;
}

float lireTemperature() {
  sondes.requestTemperatures();
  float t = sondes.getTempCByIndex(0);
  return (t == DEVICE_DISCONNECTED_C) ? NAN : t;
}

// ------------------------------------------------------------------
// File locale bornee (LittleFS ou carte SD)
// ------------------------------------------------------------------
uint32_t compterLignes() {
  File f = stockage.open(FICHIER_FILE, FILE_READ);
  if (!f) return 0;

  uint32_t n = 0;
  while (f.available()) {
    String ligne = f.readStringUntil('\n');
    ligne.trim();
    if (ligne.length() > 2) n++;
  }
  f.close();
  return n;
}

/**
 * Supprime les `n` premieres lignes de la file en reecrivant le fichier
 * une seule fois (limite l'usure de la memoire flash).
 */
uint32_t supprimerPremieresLignes(uint32_t n) {
  if (n == 0) return 0;

  File src = stockage.open(FICHIER_FILE, FILE_READ);
  if (!src) return 0;

  File tmp = stockage.open(FICHIER_TMP, FILE_WRITE);
  if (!tmp) {
    src.close();
    return 0;
  }

  uint32_t index = 0;
  uint32_t restantes = 0;
  while (src.available()) {
    String ligne = src.readStringUntil('\n');
    ligne.trim();
    if (ligne.length() < 3) continue;
    index++;
    if (index <= n) continue;
    tmp.println(ligne);
    restantes++;
  }
  src.close();
  tmp.close();

  stockage.remove(FICHIER_FILE);
  stockage.rename(FICHIER_TMP, FICHIER_FILE);

  uint32_t supprimees = index - restantes;
  compteurFiles = restantes;
  return supprimees;
}

void enfilerMesure(const String& charge) {
  // File bornee : on sacrifie les mesures les plus anciennes plutot que
  // de saturer la memoire (une citerne en panne reseau pendant des jours
  // ne doit pas remplir la flash).
  if (compteurFiles >= MAX_QUEUE) {
    uint32_t purge = MAX_QUEUE / 10;
    uint32_t supprimees = supprimerPremieresLignes(purge);
    Serial.printf("[stockage] file pleine : %u ancienne(s) mesure(s) abandonnee(s)\n", supprimees);
  }

  File f = stockage.open(FICHIER_FILE, FILE_APPEND);
  if (!f) {
    Serial.println("[stockage] impossible d'ouvrir la file locale");
    return;
  }
  f.println(charge);
  f.close();
  compteurFiles++;
  Serial.printf("[stockage] mesure mise en file locale (%u/%u)\n", compteurFiles, MAX_QUEUE);
}

bool lireLot(String* lot, uint32_t& n, uint32_t maxLot) {
  File f = stockage.open(FICHIER_FILE, FILE_READ);
  if (!f) return false;

  n = 0;
  while (f.available() && n < maxLot) {
    String ligne = f.readStringUntil('\n');
    ligne.trim();
    if (ligne.length() > 2) lot[n++] = ligne;
  }
  f.close();
  return n > 0;
}

// ------------------------------------------------------------------
// Accuses de reception
// ------------------------------------------------------------------
void surMessageAck(char* topic, byte* payload, unsigned int length) {
  JsonDocument doc;
  if (deserializeJson(doc, payload, length)) {
    Serial.println("[ack] payload illisible");
    return;
  }

  ackHorodatage = String((const char*)(doc["horodatage"] | ""));
  ackStatut = String((const char*)(doc["statut"] | ""));
  ackRaison = String((const char*)(doc["raison"] | ""));
  ackRecu = true;
}

String extraireHorodatage(const String& charge) {
  JsonDocument doc;
  if (deserializeJson(doc, charge)) return String("");
  return String((const char*)(doc["horodatage"] | ""));
}

/**
 * Le serveur renvoie un horodatage ISO 8601 normalise (avec millisecondes).
 * On compare l'instant a la seconde pres, pas la chaine brute : sinon un
 * horodatage emis sans millisecondes ne correspondrait jamais a l'accuse.
 */
bool memeInstant(const String& a, const String& b) {
  if (a.length() == 0 || b.length() == 0) return false;
  return a.substring(0, 19) == b.substring(0, 19);
}

/**
 * Publie une mesure puis attend l'ack correspondant (meme horodatage).
 * Renvoie false si aucun ack n'arrive : la mesure doit alors rester en file.
 */
bool publierEtAttendreAck(const String& charge, String& statut, String& raison) {
  String attendu = extraireHorodatage(charge);
  ackRecu = false;

  // PubSubClient ne publie qu'en QoS 0 : la garantie vient de l'ack applicatif.
  if (!mqtt.publish(TOPIC_MESURES, charge.c_str(), false)) return false;

  unsigned long debut = millis();
  while (millis() - debut < ACK_TIMEOUT_MS) {
    mqtt.loop();
    // Horodatage vide (pas de NTP) : le serveur repond avec l'heure de
    // reception, on accepte donc le premier accuse recu.
    bool correspond = attendu.length() == 0 || memeInstant(ackHorodatage, attendu);
    if (ackRecu && correspond) {
      statut = ackStatut;
      raison = ackRaison;
      return true;
    }
    delay(10);
  }
  return false;
}

/**
 * Rejoue un lot de mesures stockees. Une mesure n'est retiree de la file
 * qu'apres un ack (OK, DUPLICATE ou REJECTED) : un REJECTED est un message
 * definitivement invalide (horodatage aberrant, equipement inconnu), le
 * conserver bloquerait la file pour toujours.
 */
void resynchroniser() {
  if (compteurFiles == 0) return;

  String lot[RESYNC_LOT];
  uint32_t n = 0;
  if (!lireLot(lot, n, RESYNC_LOT)) return;

  uint32_t acquittees = 0;
  for (uint32_t i = 0; i < n; i++) {
    String statut;
    String raison;
    if (!publierEtAttendreAck(lot[i], statut, raison)) {
      Serial.println("[stockage] pas d'accuse : file conservee pour plus tard");
      break;
    }
    acquittees++;
    if (raison.length() > 0) {
      Serial.printf("[stockage] rejoue : %s (%s)\n", statut.c_str(), raison.c_str());
    } else {
      Serial.printf("[stockage] rejoue : %s\n", statut.c_str());
    }
    delay(50);
  }

  if (acquittees > 0) {
    supprimerPremieresLignes(acquittees);
    Serial.printf("[stockage] %u mesure(s) acquittee(s), %u restante(s)\n", acquittees, compteurFiles);
  }
}

// ------------------------------------------------------------------
// Reseau
// ------------------------------------------------------------------
void connecterWifi() {
  if (WiFi.status() == WL_CONNECTED) return;

  Serial.printf("[wifi] connexion a %s", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, WIFI_CHANNEL);

  unsigned long debut = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - debut < 15000) {
    delay(250);
    Serial.print(".");
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\n[wifi] connecte, IP %s\n", WiFi.localIP().toString().c_str());
    configTime(0, 0, "pool.ntp.org", "time.nist.gov");
    for (int i = 0; i < 20 && !heureValide(); i++) {
      delay(250);
      synchroniserHeure();
    }
  } else {
    Serial.println("\n[wifi] echec de connexion");
  }
}

void connecterMqtt() {
  if (mqtt.connected()) return;

  String clientId = String(DEVICE_ID) + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  Serial.printf("[mqtt] connexion a %s ...", MQTT_HOST);

  // Last Will : le serveur marque l'unite hors ligne si elle disparait
  String willPayload = String("{\"unite\":\"") + DEVICE_ID + "\",\"statut\":\"HORS_LIGNE\"}";

  const char* mqttUser = strlen(MQTT_USER) > 0 ? MQTT_USER : nullptr;
  const char* mqttPass = strlen(MQTT_PASSWORD) > 0 ? MQTT_PASSWORD : nullptr;

  if (mqtt.connect(clientId.c_str(), mqttUser, mqttPass, TOPIC_STATUT, 1, true, willPayload.c_str())) {
    Serial.println(" connecte");
    mqtt.publish(TOPIC_STATUT, String("{\"unite\":\"" + String(DEVICE_ID) + "\",\"statut\":\"EN_LIGNE\"}").c_str(), true);
    mqtt.subscribe(TOPIC_ACK, 1);
    derniereResync = 0;   // provoque une reprise immediate de la file
  } else {
    Serial.printf(" echec (rc=%d)\n", mqtt.state());
  }
}

// ------------------------------------------------------------------
// Acquisition + publication
// ------------------------------------------------------------------
String construirePayload(float ph, float turbidite, float temperature,
                         float conductivite, float niveauCm, float niveauPct,
                         const String& source) {
  JsonDocument doc;
  doc["unite"] = DEVICE_ID;
  doc["type"] = DEVICE_TYPE;
  doc["code"] = DEVICE_CODE;
  // Sans NTP, l'horodatage est vide : le serveur applique l'heure de
  // reception et marque la mesure avec source = "RECU".
  doc["horodatage"] = heureValide() ? horodatageIso() : "";
  doc["source"] = source;

  JsonObject m = doc["mesures"].to<JsonObject>();
  m["ph"] = round(ph * 100) / 100.0;
  m["turbidite"] = round(turbidite * 100) / 100.0;
  m["temperature"] = round(temperature * 100) / 100.0;
  m["conductivite"] = round(conductivite * 100) / 100.0;
  m["niveauCm"] = round(niveauCm * 10) / 10.0;
  m["niveauPct"] = round(niveauPct * 10) / 10.0;

  String sortie;
  serializeJson(doc, sortie);
  return sortie;
}

void acquerirEtPublier() {
  float distance = lireDistanceCm();
  float temperature = lireTemperature();
  float ph = lirePh();
  float turbidite = lireTurbidite();
  float conductivite = lireConductivite();

  bool mesureValide = distance > 0 && !isnan(temperature) &&
                      ph >= 0 && ph <= 14 &&
                      turbidite >= 0 && turbidite <= 3000 &&
                      conductivite >= 0 && conductivite <= 50000 &&
                      temperature >= -20 && temperature <= 80;

  if (!mesureValide) {
    Serial.println("[capteur] mesures invalides, cycle ignore");
    return;
  }

  float niveauCm = HAUTEUR_CITERNE_CM - distance;
  if (niveauCm < 0) niveauCm = 0;
  if (niveauCm > HAUTEUR_CITERNE_CM) niveauCm = HAUTEUR_CITERNE_CM;
  float niveauPct = (niveauCm / HAUTEUR_CITERNE_CM) * 100.0;

  String charge = construirePayload(ph, turbidite, temperature, conductivite,
                                    niveauCm, niveauPct, "MQTT");

  bool reseauDisponible = WiFi.status() == WL_CONNECTED && mqtt.connected();
  if (!reseauDisponible) {
    enfilerMesure(charge);
    return;
  }

  String statut;
  String raison;
  if (!publierEtAttendreAck(charge, statut, raison)) {
    // Pas d'accuse : le message est peut-etre arrive. Il sera renvoye plus
    // tard ; le serveur le reconnaitra comme doublon (idempotence).
    enfilerMesure(charge);
    return;
  }

  if (statut == "REJECTED") {
    Serial.printf("[mesure] rejetee par le serveur : %s\n", raison.c_str());
    return;
  }

  compteurEnvois++;
  Serial.printf("[mesure] #%u pH=%.2f turb=%.1f NTU T=%.1f C cond=%.0f uS/cm niveau=%.0f%% (%s)\n",
                compteurEnvois, ph, turbidite, temperature, conductivite, niveauPct, statut.c_str());
}

// ------------------------------------------------------------------
// Setup / loop
// ------------------------------------------------------------------
void setup() {
  Serial.begin(115200);
  delay(200);
  Serial.println("\n=== AQUAMONITOR - unite embarquee (Wokwi) ===");

  pinMode(PIN_TRIG, OUTPUT);
  pinMode(PIN_ECHO, INPUT);

  analogReadResolution(12);
  sondes.begin();

#if USE_SD_CARD
  if (!SD.begin(PIN_SD_CS)) {
    Serial.println("[stockage] carte SD indisponible");
  } else {
    compteurFiles = compterLignes();
    Serial.printf("[stockage] carte SD prete (%u mesure(s) en attente)\n", compteurFiles);
  }
#else
  if (!LittleFS.begin(true)) {
    Serial.println("[stockage] LittleFS indisponible");
  } else {
    compteurFiles = compterLignes();
    Serial.printf("[stockage] LittleFS pret (%u mesure(s) en attente)\n", compteurFiles);
  }
#endif

  connecterWifi();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  mqtt.setKeepAlive(30);
  mqtt.setCallback(surMessageAck);
  connecterMqtt();

  Serial.printf("[config] unite %s / %s / %s\n", DEVICE_ID, DEVICE_TYPE, DEVICE_CODE);
}

void loop() {
  connecterWifi();
  if (WiFi.status() == WL_CONNECTED) connecterMqtt();

  if (millis() - derniereMesure >= INTERVALLE_MS) {
    derniereMesure = millis();
    acquerirEtPublier();
  }

  // Reprise progressive de la file locale, sans bloquer l'acquisition.
  if (mqtt.connected() && compteurFiles > 0 &&
      millis() - derniereResync >= RESYNC_PERIODE_MS) {
    derniereResync = millis();
    resynchroniser();
  }

  mqtt.loop();
}
