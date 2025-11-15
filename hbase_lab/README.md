# TP Hadoop – HBase – Spark  
## 📘 Rapport & Screenshots

Ce dossier contient les programmes Java développés pour interagir avec **HBase via Spark**, ainsi que les captures d’écran des différentes étapes du TP : importation des données, exécutions Spark et vérifications dans HBase/Hadoop.

---

# 1️⃣ Importation des données dans HBase

### 🟦 Import du fichier et chargement dans HBase
![Importation HDFS / HBase](image-1.png)

### 🟦 Vérification du chargement
![Vérification du chargement](image-2.png)

### 🟦 Exemple de scan
![scan 'products', {LIMIT => 5}](image-3.png)

---

# 2️⃣ HbaseSparkProcess – Count des lignes

### ▶️ Lancement du job Spark
![spark-submit](image-4.png)

### ✔️ Résultat obtenu
![Résultat](image-5.png)

---

# 3️⃣ HbaseSparkSum – Somme des prix

![Résultat somme](image-6.png)

---

# 4️⃣ HbaseSparkTOPN – Top des prix

![TOP N Résultat](image-7.png)

---

# 5️⃣ HbaseSparkAvg – Moyenne des prix

![Résultat moyenne](image-8.png)

---

📌 **Fin du rapport — tous les traitements Spark + HBase ont été exécutés avec succès.**  
