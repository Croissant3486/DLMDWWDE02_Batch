# DLMDWWDE02_Batch

Herzlich Willkommen zum GitHub Repository für die Batchprozessierungsaufgabe im Kurs DLMDWWDE02 (Data Engineering) der IU. In dieser README finden Sie einen Einstieg in die Struktur des Repositories und des Codes.



Aufbau:

  1. Ordner "Phase 1"
     
     Enthält alle Dokumente der "Konzeptionsphase". Darunter zählen das Flowchart zur geplanten Datenarchitektur, sowie eine kurze Diskussion zu den eingesetzen Elementen inklusive Berücksichtigung von Verfügbarkeit (reliability), Skalierbarkeit (scalability), Wartbarkeit (maintainability), Datensicherheit, Datenschutz und Data Governance.

     Die zu prozessierenden Datensätze basieren auf mehr als 1.000.000 gemessenen Temperaturdaten von deutschen Wetterstationen der Jahre 1996-2021 (https://www.kaggle.com/datasets/matthiaskleine/german-temperature-data-1990-2021/data). Um Prognosen zur Temperaturentwicklung in Deutschland in den nächsten Jahren mittels Machine Learning zu ermöglichen, müssen diese Datensätze verarbeitet, bereinigt und aggregiert werden. Alle beschriebenen Komponenten werden als Microservices implementiert. Um eine Isolation unter den Diensten sicherzustellen sowie eine erhöhte Ausfallsicherheit/Wartbarkeit zu gewährleisten, sollen alle Microservices in separaten Docker Containern laufen.
      
  3. Ordner "Phase 2"

  4. Ordner "Phase 3"   
