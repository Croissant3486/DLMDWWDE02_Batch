# DLMDWWDE02_Batch

Herzlich Willkommen zum GitHub Repository für die Batchprozessierungsaufgabe im Kurs DLMDWWDE02 (Data Engineering) der IU. In dieser README finden Sie einen Einstieg in die Struktur des Repositories und des Codes.



Aufbau:

  1. Ordner "Phase 1"
     
     Enthält alle Dokumente der "Konzeptionsphase". Darunter zählen das Flowchart zur geplanten Datenarchitektur, sowie eine kurze Diskussion zu den eingesetzen Elementen inklusive Berücksichtigung von Verfügbarkeit (reliability), Skalierbarkeit (scalability), Wartbarkeit (maintainability), Datensicherheit, Datenschutz und Data Governance.

     Die zu prozessierenden Datensätze basieren auf mehr als 1.000.000 gemessenen Temperaturdaten von deutschen Wetterstationen der Jahre 1996-2021 (https://www.kaggle.com/datasets/matthiaskleine/german-temperature-data-1990-2021/data). Um Prognosen zur Temperaturentwicklung in Deutschland in den nächsten Jahren mittels Machine Learning zu ermöglichen, müssen diese Datensätze verarbeitet, bereinigt und aggregiert werden. Alle beschriebenen Komponenten werden als Microservices implementiert. Um eine Isolation unter den Diensten sicherzustellen sowie eine erhöhte Ausfallsicherheit/Wartbarkeit zu gewährleisten, sollen alle Microservices in separaten Docker Containern laufen.
      
  2. Ordner "Phase 2"

     Enthält alle Dokumente der "Erarbeitungs-/Reflexionsphase". Der gesamte Code wird in diesem GitHub Repository veröffentlicht und so erstellt, dass dieser Reproduzierbar ist. Es wird bei der Implementation darauf geachtet, dass alle Elemente die in der "Konzeptionsphase" erarbeitet worden sind in diesem Abschnitt Anwendung finden.


     Die Laufzeit der Batchverarbeitung inklusive der Visualisierung beträgt circa 85 Minuten. Diese wurde auf der folgende Hardware Konfiguration bestätigt:

     CPU: Intel Core i5 13600K

     RAM: 32GB DDR-5 6000 MT/s
     
     GPU: Nvidia RTX 3080
     
     OS: Windows 11 23H2
     
     SSD: Samsung 990 PRO M.2 PCIe 4

     WICHTIG:
     Um eine korrekte Ausführung des Code zu gewährleisten muss zunächst die ZIP-Datei im "Input" Ordner entpackt werden. Dies ist leider notwendig, da GitHub nur eine Upload Dateigröße von <100 MB zulässt.

     In diesem Repository befinden sich eine Docker Compose Datei für die Erstellung der drei Microservices. Für den Ingest wird ein "Kafka" Container verwendet, für die Prozessierung ein Spark Container und für die Persistierung ein HDFS Container. Die Inputdaten wurden bereits in Punkt 1. erwähnt. Der verarbeitete Output wird in einem HDFS gespeichert, welches über die URL: http://localhost:9870 erreichbar ist. Zur Erstellung der Container wurde die Docker-CLI sowie Docker Desktop verwendet.

     Der Spark-Service beginnt 100 Sekunden nachdem der Kafka Producer aufgehört hat neue Daten zu übermitteln die bereits aggregierten Daten zu visualieren. Danach beendeten sich der Spark Service von selbst. Sind alle Daten aus dem HDFS verabeitet und visualisiert worden, so können diese im "Output" Ordner des heruntergeladenen Repos angeschaut werden.

     Anleitung zur erfolgreichen Batchverarbeitung der Daten (sofern Docker inkl. Docker-Compose installiert sind):

     1. Die ZIP-Datei unter folgendem Pfad im selbigen Ordner entpacken: "input\german_temperature_data_1996_2021_from_selected_weather_stations.zip"
    
     2. Per beliebigem Commandline Tool im administrativen Kontext zum Ordner "Phase 2" navigieren.
    
     3. Ausführen der Kommandozeile "docker-compose up --always-recreate-deps --detach --force-recreate"
    
     4. Warten bis die Verarbeitung abgeschlossen ist (dies dauert etwa 85 Minuten, siehe oben)
       
     5. Nachdem der lokale "Output"-Ordner innerhalb des Repositories mit diversen *.png Dateien gefüllt ist, ist die Verarbeitung erfolgreich abgeschlossen. Die PNG Dateien enthalten die Aggregationen (Mean, Median, Mode) innerhalb eines Jahres der ausgewählten Wetterstationsnummer. Alle Werte können im Detail aus dem HDFS unter folgendem Link: http://localhost:9870/explorer.html#/tmp/hadoop-root/dfs/data/processed_data.csv ausgelesen werden.

  4. Ordner "Phase 3"   
