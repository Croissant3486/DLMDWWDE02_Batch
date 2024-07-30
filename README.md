# DLMDWWDE02_Batch

Herzlich Willkommen zum GitHub Repository für die Batchprozessierungsaufgabe im Kurs DLMDWWDE02 (Data Engineering) der IU. In dieser README finden Sie einen Einstieg in die Struktur des Repositories und des Codes.



Aufbau:

  1. Ordner "Phase 1"
     
     Enthält alle Dokumente der "Konzeptionsphase". Darunter zählen das Flowchart zur geplanten Datenarchitektur, sowie eine kurze Diskussion zu den eingesetzen Elementen inklusive Berücksichtigung von Verfügbarkeit (reliability), Skalierbarkeit (scalability), Wartbarkeit (maintainability), Datensicherheit, Datenschutz und Data Governance.

     Die zu prozessierenden Datensätze basieren auf mehr als 1.000.000 gemessenen Temperaturdaten von deutschen Wetterstationen der Jahre 1996-2021 (https://www.kaggle.com/datasets/matthiaskleine/german-temperature-data-1990-2021/data). Um Prognosen zur Temperaturentwicklung in Deutschland in den nächsten Jahren mittels Machine Learning zu ermöglichen, müssen diese Datensätze verarbeitet, bereinigt und aggregiert werden. Alle beschriebenen Komponenten werden als Microservices implementiert. Um eine Isolation unter den Diensten sicherzustellen sowie eine erhöhte Ausfallsicherheit/Wartbarkeit zu gewährleisten, sollen alle Microservices in separaten Docker Containern laufen.
      
  3. Ordner "Phase 2"

     Enthält alle Dokumente der "Erarbeitungs-/Reflexionsphase". Der gesamte Code wird in diesem GitHub Repository veröffentlicht und so erstellt, dass dieser Reproduzierbar ist. Es wird bei der Implementation darauf geachtet, dass alle Elemente die in der "Konzeptionsphase" erarbeitet worden sind in diesem Abschnitt Anwendung finden.

     WICHTIG:
     Um eine korrekte Ausführung des Code zu gewährleisten muss zunächst die ZIP-Datei im "Input" Ordner entpackt werden. Dies ist leider notwendig, da GitHub nur eine Upload Datei größe von <100 MB zulässt.

     Im Repo befinden sich eine Docker Compose Datei für die Erstellung der drei Microservices. Für den Ingest wird ein "Kafka" Container verwendet, für die Prozessierung ein Spark Container und für die Persistierung ein HDFS Container. Die Inputdaten wurden bereits in Punkt 1. erwähnt. Der Output wird statistische Maße enthalten inklusive einer Visualisierung der Daten mittels eines Graphen.

  5. Ordner "Phase 3"   
