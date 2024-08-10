# DLMDWWDE02_Batch

Herzlich Willkommen zum GitHub Repository für die Batchprozessierungsaufgabe im Kurs DLMDWWDE02 (Data Engineering) der IU. In dieser README finden Sie einen Einstieg in die Struktur des Repositories und des Codes.

##### Table of Contents 
- [DLMDWWDE02_Batch](#dlmdwwde02_batch)
  * [Ordner Phase 1](#ordner-phase-1)
  * [Ordner Phase 2](#ordner-phase-2)
    * [Anleitung](#anleitung)
    * [Erklärung der Unterordner](#erklärung-der-unterordner)
      * [backups](#backups)
      * [hdfs](#hdfs)
      * [input](#input)
      * [kafka](#kafka)
      * [output](#output)
      * [spark](#spark)
    * [Weitere Informationen](#weitere-informationen)
  * [Ordner Phase 3](#ordner-phase-3)
  


## Ordner Phase 1
     
   Enthält alle Dokumente der "Konzeptionsphase". Darunter zählen das Flowchart zur geplanten Datenarchitektur, sowie eine kurze Diskussion zu den eingesetzen Elementen inklusive Berücksichtigung von Verfügbarkeit (reliability), Skalierbarkeit (scalability), Wartbarkeit (maintainability), Datensicherheit, Datenschutz und Data Governance.

  Die zu prozessierenden Datensätze basieren auf mehr als 1.000.000 gemessenen Temperaturdaten von deutschen Wetterstationen der Jahre 1996-2021 (https://www.kaggle.com/datasets/matthiaskleine/german-temperature-data-1990-2021/data). Um Prognosen zur Temperaturentwicklung in Deutschland in den nächsten Jahren mittels Machine Learning zu ermöglichen, müssen diese Datensätze verarbeitet, bereinigt und aggregiert werden. Alle beschriebenen Komponenten werden als Microservices implementiert. Um eine Isolation unter den Diensten sicherzustellen sowie eine erhöhte Ausfallsicherheit/Wartbarkeit zu gewährleisten, sollen alle Microservices in separaten Docker Containern laufen.
      
## Ordner Phase 2

  Enthält alle Dokumente der "Erarbeitungs-/Reflexionsphase". Der gesamte Code wird in diesem GitHub Repository veröffentlicht und so erstellt, dass dieser Reproduzierbar ist. Es wird bei der Implementation darauf geachtet, dass alle Elemente die in der "Konzeptionsphase" erarbeitet worden sind in diesem Abschnitt Anwendung finden.


  Die Laufzeit der Batchverarbeitung inklusive der Visualisierung beträgt circa 80 Minuten. Diese wurde auf der folgende Hardware Konfiguration bestätigt:



  | Hardwarekomponente | Bezeichnung | 
  | ------------- |:-------------:|
  | CPU    | Intel Core i5 13600K |
  | GPU    | Nvidia RTX 3080 |
  | RAM | 32GB DDR-5 6000 MT/s | 
  | SSD | Samsung 990 PRO M.2 PCIe 4 | 
  | OS | Windows 11 23H2 | 

   `WICHTIG:`
     Um eine korrekte Ausführung des Code zu gewährleisten muss zunächst die ZIP-Datei im "Input" Ordner entpackt werden. Dies ist leider notwendig, da GitHub nur eine Upload Dateigröße von <100 MB zulässt.

  In diesem Repository befinden sich eine Docker Compose Datei für die Erstellung der drei Microservices. Für den Ingest wird ein "Kafka" Container verwendet, für die Prozessierung ein Spark Container und für die Persistierung ein HDFS Container. Die Inputdaten wurden bereits in Punkt 1. erwähnt. Der verarbeitete Output wird in einem HDFS gespeichert, welches über die URL: http://localhost:9870 erreichbar ist. Zur Erstellung der Container wurde die Docker-CLI sowie Docker Desktop verwendet.

  Der Spark-Service beginnt 120 Sekunden nachdem der Kafka Producer aufgehört hat neue Daten zu übermitteln die aggregierten Daten aus dem HDFS zu visualieren. Danach beendeten sich der Spark Service von selbst. Sind alle Daten aus dem HDFS verabeitet und visualisiert worden, so können diese im "Output" Ordner des heruntergeladenen Repos angeschaut werden.

### Anleitung
  Anleitung zur erfolgreichen Batchverarbeitung der Daten (sofern Docker inkl. Docker-Compose installiert sind):

  1. Klonen des Git Repositories in ein lokales Verzeichnis.

  2. Die ZIP-Datei unter folgendem Pfad im selbigen Ordner entpacken: "input\german_temperature_data_1996_2021_from_selected_weather_stations.zip" => "input\german_temperature_data_1996_2021_from_selected_weather_stations.csv"

  3. Per beliebigem Commandline Tool im administrativen Kontext zum Ordner "Phase 2" navigieren.
    
  4. Ausführen der Kommandozeile "docker-compose up --always-recreate-deps --detach --force-recreate"
    
  5. Warten bis die Verarbeitung abgeschlossen ist (dies dauert etwa 80 Minuten, siehe oben).
       
  6. Nachdem der lokale "Output"-Ordner innerhalb des Repositories mit diversen *.png Dateien gefüllt ist, ist die Verarbeitung erfolgreich abgeschlossen. Die PNG-Dateien enthalten die Aggregationen (Mean, Median, Mode) innerhalb eines Jahres der ausgewählten Wetterstationsnummer. Alle Werte können im Detail aus dem HDFS unter folgendem Link: http://localhost:9870/explorer.html#/tmp/hadoop-root/dfs/data/processed_data.csv heruntergeladen und ausgelesen werden.


### Erklärung der Unterordner
Hier finden Sie eine Kurzbeschreibung der Inhalte der Unterordner des Projekts "Phase 2".

#### backups

Dieser Ordner enthält alle Konfigurationen für den HDFS Backup Container.

#### hdfs

Enthält die Konfiguration für die diversen HDFS Container aus der docker-compose.yml.

#### input

Enthält die Input Daten für die Batchverarbeitung, hier als ZIP Datei, da GitHub keine größeren Dateien akzeptiert. Bitte beachten Sie hierfür Punkt 2. der Installationsanleitung.

#### kafka

Hier liegen der Kafka Producer, die benötigten dependencies und das Dockerfile für die Custom Konfiguration.

#### output

Ist mit einer Beispieldatei gefüllt, um die Visualisierung der Temperatur Aggregationen darzustellen und damit GitHub diesen Ordner ins Repo aufnimmt. Enthält nach der Verarbeitung > 1000 Visualisierungen der einzelnen Wetterstationen von 1996-2021.

#### spark

Enthält alle Konfigurationsdateien sowie den PySpark Service für das Projekt.

### Weitere Informationen
Das GitHub Repo beinhaltet noch eine zweite Branch mit dem Namen "Security". In dieser werden Einstellung zur sicheren Übertragung per SSL und ACLs für das HDFS konfiguriert. Dies wird hier nur Beispielhaft dargestellt, da man für eine echte Security, echte Zertifikate erstellen müsste, welche den Rahmen der Umsetzung sprengen würden. Auch ist das setzen der ACLs nicht direkt über Docker-Compose möglich, weshalb in diesem Fall davon abgesehen wird. Dennoch ist der Zugriff auf bestimmte User (z.B. hadoop) beschränkt und es kann nicht jeder alles tun. Der Web-User des HDFS kann bspw. nur Daten einsehen, aber nicht verändern, löschen oder anlegen. Die "main" Branch ist somit die, die für die Reproduzierbarkeit der Ergebnisse verwendet werden sollte.

## Ordner Phase 3

In diesem Ordner wird die Synthese des gesamten Portfolios dargestellt und ein abschließendes Abstract zur Verfügung gestellt. Dies geschieht nach dem Feedback aus "Phase 2".
