# DLMDWWDE02_Batch

Herzlich Willkommen im GitHub Repository für die Batchprozessierungsaufgabe des Kurses DLMDWWDE02 (Data Engineering) der IU. In dieser README finden Sie einen Einstieg in die Struktur des Repositories und des Codes.

##### Table of Contents 
- [DLMDWWDE02_Batch](#dlmdwwde02_batch)
    * [Anleitung](#anleitung)
    * [Erklärung der Unterordner](#erklärung-der-unterordner)
      * [backups](#backups)
      * [hdfs](#hdfs)
      * [input](#input)
      * [kafka](#kafka)
      * [output](#output)
      * [spark](#spark)
      * [visuals](#visuals)
    * [Weitere Informationen](#weitere-informationen)


  Diese Repository enthält den gesamten Code des Batch-Prozessierungsprojekts.
  
  Die Laufzeit der Batchverarbeitung inklusive der Visualisierung beträgt circa 80 Minuten. Diese wurde auf der folgende Hardware Konfiguration bestätigt:


  | Hardwarekomponente | Bezeichnung | 
  | ------------- |:-------------:|
  | CPU    | Intel Core i5 13600K |
  | GPU    | Nvidia RTX 3080 |
  | RAM | 32GB DDR-5 6000 MT/s | 
  | SSD | Samsung 990 PRO M.2 PCIe 4 | 
  | OS | Windows 11 23H2 | 

   `WICHTIG:`
     Um eine korrekte Ausführung des Code zu gewährleisten, muss zunächst die ZIP-Datei im "Input" Ordner entpackt werden. Dies ist leider notwendig, da GitHub nur eine Upload Dateigröße von <100 MB zulässt.

  In diesem Repository befinden sich eine Docker Compose Datei für die Erstellung der Microservices. Für den Ingest wird ein "Kafka" Container verwendet, für die Prozessierung ein Spark Container, für die Persistierung ein HDFS Container und für die Visualisierung ein PySpark Container. Des weiteren exisitiert ein Container der sich auf das Backup des HDFS fokussiert und diverse Container die für den Betrieb der erwähnten Microservices benötigt werden. Die zu prozessierenden Input-Datensätze basieren auf mehr als 1.000.000 gemessenen Temperaturdaten von deutschen Wetterstationen der Jahre 1996-2021 (https://www.kaggle.com/datasets/matthiaskleine/german-temperature-data-1990-2021/data). Der verarbeitete Output des Spark Services wird in einem HDFS gespeichert, welches über die URL: http://localhost:9870 erreichbar ist. Der Zustand des Spark Services kann über den Spark-Master überprüft werden. Dieser ist über die Adresse: http://localhost:8080 erreichbar. Der PySpark Visualisierungsmicroservice sorgt dafür, dass nachdem keine weiteren zu verarbeitenden Datensätze mehr in die Kafka Topic "Temperature" geschrieben werden Graphen auf Basis des Jahres und der Wetterstation erzeugt werden. Der erzeugten Graphen wernen im Anschluss in den Ordner "Output" geschrieben. Zur Erstellung der Container wurde die Docker-CLI sowie Docker Desktop verwendet.

  Der Visualisierungs-Service beginnt 120 Sekunden nachdem der Kafka Producer aufgehört hat neue Daten an die Topic zu übermitteln die aggregierten Daten aus dem HDFS mittels MatPlotLib und Seaborn zu visualisieren. Im Anschluss wird solange gewartet bis erneut Datensätze in die Kafka Topic geschrieben werden. Nach erneutem Auslaufen des gesetzten Timeouts von 120 Sekunden, werden ebenfalls wieder Visualisierungen erzeugt.

  Sind alle Daten aus dem HDFS (zum Zeitpunkt des Timeout Triggers) verarbeitet und visualisiert worden, so können diese im "Output" Ordner des heruntergeladenen Repositories angeschaut werden. Der Service läuft kontinuierlich und stoppt nur bei kritischen Fehlern. Der Timeout zur Visualisierung der Daten ist sinnvoll, da die aggregierten Metriken über den Verlauf eines Jahres dargestellt werden sollen. Würde der Service kontinuierlich visualisieren, so würden die Graphen entweder dauerhaft überschrieben werden, was inperformant ist oder unvollständig sein, was für eine Visualisierung ebenfalls unbrauchbar ist.

Alle Microservices laufen kontinuierlich und warten auf neue Inputs. Entsprechende Timeout-Werte sorgen dafür, dass nicht zu oft geprüft wird und somit Systemressourcen geschont werden. Der Kafka Producer Service stoppt bewusst nach der Verarbeitung der Batches, um eine Veränderung der Inputdaten zu ermöglichen (wie es bei einer produktiven Batch Prozessierung der Fall wäre). Sind Änderungen vorgenommen worden, so kann der Container wiederholt angetriggert werden und die Verarbeitung startet erneut. 

## Anleitung
  Anleitung zur erfolgreichen Batchverarbeitung der Daten (sofern Docker inkl. Docker-Compose installiert sind):

  1. Klonen des Git Repositories in ein lokales Verzeichnis.

  2. Die ZIP-Datei unter folgendem Pfad im selbigen Ordner entpacken: "input\german_temperature_data_1996_2021_from_selected_weather_stations.zip" => "input\german_temperature_data_1996_2021_from_selected_weather_stations.csv"

  3. Per beliebigem Commandline Tool im administrativen Kontext zum Projektordner (auf der Ebene des docker-compose.yml Files) navigieren.
    
  4. Ausführen der Kommandozeile "docker-compose up --always-recreate-deps --detach --force-recreate"
    
  5. Für Ergebnisse: Warten bis die Verarbeitung abgeschlossen ist (dies dauert etwa 80 Minuten, siehe oben). Wenn andere Wetterdaten verwendet werden, können die Zeiten variieren.
       
  6. Nachdem der lokale "Output"-Ordner innerhalb des Repositories mit diversen *.png Dateien gefüllt ist, ist die aktuelle Verarbeitung erfolgreich abgeschlossen. Die PNG-Dateien enthalten die Aggregationen (Mean, Median, Mode) innerhalb eines Jahres der ausgewählten Wetterstationsnummer. Alle Werte können im Detail aus dem HDFS unter folgendem Link: http://localhost:9870/explorer.html#/tmp/hadoop-root/dfs/data/processed_data.csv heruntergeladen und ausgelesen werden.

  7. Werden Daten im Inputverzeichnis angepasst / verändert und der Kafka Producer Container neugestartet, dann beginnt der gesamte Verarbeitungsprozess erneut.


## Erklärung der Unterordner
Hier finden Sie eine Kurzbeschreibung der Inhalte der Unterordner des Projekts.

### backups

Dieser Ordner enthält alle Konfigurationen für den HDFS Backup Container und legt einen Cron Job zur Sicherung in einem "Archiv" an.

### hdfs

Enthält die Konfiguration für die diversen HDFS Container (NameNode + DataNode) aus der docker-compose.yml. Sorgt für die Funktionalität des HDFS.

### input

Enthält die Input Daten für die Batchverarbeitung, hier als ZIP Datei, da GitHub keine größeren Dateien akzeptiert. Bitte beachten Sie hierfür Punkt 2. der Installationsanleitung.

### kafka

Hier liegt der Kafka Producer, die benötigten Dependencies und das Dockerfile für die Custom Konfiguration. Dieser Service zerteilt die Inputdaten als Batches und veröffentlicht diese in der Kafka Topic "Temperature".

### output

Ist mit einer Beispieldatei (beispiel.png) gefüllt, um die Visualisierung der Temperatur Aggregationen darzustellen und damit GitHub diesen Ordner in das Repository aufnimmt. Enthält nach der Verarbeitung > 1000 Visualisierungen der einzelnen Wetterstationen von 1996-2021. Die Beispieldatei wurde nach dem erfolgreichen Abschluss eines Verarbeitungszyklus aller Datensätze extrahiert und für die Darstellung eines Graphen als Beispiel in diesem Repository abgelegt. Alle Metadaten (Stationsnummer, Jahr) finden sich direkt in der PNG Datei und können nach erfolgreicher Verarbeitung mit dem neu erzeugten Graphen abgeglichen werden. Diese findet sich im entsprechenden Zielordner unter Stationsnummer (164) + Jahr (2016). Weicht dieser Graph leicht vom Beispiel-Graphen ab, so ist dies auf Rundungsfehler der Float Werte beim Erstellen der Graphenpunkte sowie "Random-Seeds" bei der Erzeugung der Visualisierung des Datenpunkte über MatPlotLib und Seaborn zurückzuführen.

### spark

Enthält alle Konfigurationsdateien sowie den PySpark Service für das Projekt. Dieser Service konsumiert die Datenbatches aus der Kafka Topic "Temperature" und nimmt die Aggregationen der Daten (Mean, Median, Mode etc.) vor. Nach der Aggregation werden die berechneten Werte im HDFS als CSV-Daten abgelegt.

### visuals

Enthält alle Konfigurationen sowie den Pyspark Service zur Visualisierung. Aufgerufen wird dieser über das Docker-Compose File, als separater Service-Container. Dieser Service prüft die Kafka Topic "Temperature" solange bis keine neuen Daten inklusive eines Timeouts (120 Sekunden) veröffentlicht worden sind. Anschließnd wird mit der Erzeugung der Visualisierungen aus den aggriegerten Daten, welche im HDFS abgelegt worden sind, begonnen. Dies passiert gestaffelt (für je ein Jahr inklusive aller Monate) einer Wetterstation und wird automatisch durch MatPlotLib und Seaborn in ein ansprechendes Format gepackt. Die Speicherung erfolgt im Output Ordner. Erläuterungen warum erst nach der eigentlichen Verarbeitung der Daten die Visualisierungen erzeugt werden, finden Sie im obersten Abschnitt im Bereich "WICHTIG". Nachdem dies durchgeführt worden ist, wartet der Service auf weitere Veränderungen in den Daten und beginnt erneut mit dem Visualisierungszyklus.


## Weitere Informationen
Das GitHub Repo beinhaltet noch eine zweite Branch mit dem Namen "Security". In dieser werden Einstellung zur sicheren Übertragung per SSL und ACLs für das HDFS konfiguriert. Dies wird hier nur Beispielhaft dargestellt, da man für eine echte Security, echte Zertifikate erstellen müsste, welche den Rahmen der Umsetzung sprengen würden. Auch ist das setzen der ACLs nicht direkt über Docker-Compose möglich, weshalb in diesem Fall davon abgesehen wird. Dennoch ist der Zugriff auf bestimmte User (z.B. hadoop) beschränkt und es kann nicht jeder alles tun. Der Web-User des HDFS kann bspw. nur Daten einsehen, aber nicht verändern, löschen oder anlegen. Die "main" Branch ist somit die, die für die Reproduzierbarkeit der Ergebnisse verwendet werden sollte.
