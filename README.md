

## Εγκατάσταση

Αρχικά δημιουργούμε ένα cluster με 2 nodes με δύο vm, ένα master και ένα slave. Ο master έχει public ip ενώ ο slave όχι. Η σύνδεση των δύο nodes γίνεται με local net και το κάθε μηχάνημα έχει στην διάθεση του 2 cores και 4GB ram.

## Hadoop

Η εγκατάσταση apache hadoop έγινε με βάση το `https://sparkbyexamples.com/hadoop/apache-hadoop-installation/`:
  - Αρχικά στήνουμε passwordless login με ανταλλαγή κλειδιών `ssh` μεταξύ τα 2 vm και κατεβάζουμε το `openjdk-8-jdk-headless`. 
  - Στην συνέχεια, κατεβάζουμε το hadoop 3.2.4 και αλλάζουμε τα κατάλληλα configuration files σύμφωνα με τον βοηθό εγκατάστασης *.
  - Ενημερώνουμε το bash για να υπάρχει κατάλληλο path στο hadoop directory από οποιοδήποτε directory
  
 (*) *Προσοχή, στο βήμα 3.3 στο αρχείο `hadoop/etc/hadoop/hdfs-site.xml` αντί για 3 βάζουμε 2 γιατί θέλουμε 2 datanodes και βάζουμε διαφορετικό όνομα για το αρχείο του namenode και datanode πχ `.../hdfs/data1` και `.../hdfs/data2`. Έτσι πρέπει να εκτελέσουμε το βήμα 3.6 μία φόρα για κάθε τέτοιο αρχείο. Σημαντική αλλαγή χρειάζεται και το αρχείο masters να μην έχει το 'localhost' ως μία από τις διευθύνσεις γιατί δημιουργεί πρόβληματα.*

Αν η εγκατάσταση ήταν επιτυχής, με
```bash
start-dfs.sh
```
γίνεται η εκκίνηση των hdfs clusters και η επιβεβαίωση μπορει να γίνει με την εντολή 'jps' ή πηγαίνοντας στην σελίδα `http://[master public ipv4]:9870/`

Η επικοινωνία με το hdfs file system γίνεται με εντολές τύπου `hdfs dfs -[εντολή]` πχ:
```bash
hdfs dfs -ls <args>
hdfs dfs -put <localsrc> ... <dst>
hdfs dfs -get [-ignorecrc] [-crc] <src> <localdst>
```
Η πρώτη εντολή κάνει ls σε κάποιο directory του hdfs, η δεύτερη εντολή κάνει upload ένα local file στο hdfs system και η τρίτη κάνει download ένα αρχείο του hdfs system στο local system.

## Spark

Για την εγκατάσταση του spark ακολουθούμε τις οδηγίες του φυλλάδιο που δίνεται. Κατεβάζουμε το spark version 3.1.3 που είναι συμβατό με hadoop 3.2 και python 3.8.0. Στην συνέχεια αλλάζουμε τα configuration files σύμφωνα με τον οδηγό, με την μόνη διαφορά ότι κάνουμε update το spark-defaults.conf με `spark.master spark://[local ip master]:7077` για να μην χρειάζεται με κάθε shell command να διευκρινίζουμε τον master.

Δουλεύουμε με standalone spark και ξεκινάμε αρχικά τον master με 
```bash
start-master.sh
```
από οποιοδήποτε directory επειδή έχουμε ενημερώσει το bash κατάλληλα σύμφωνα με τον οδηγό. Για να επιβεβαιώσουμε ότι η εκτέλεση είναι ορθή μπορούμε να επισκεφτούμε την σελίδα `http://[public ip master]:8080/`.

Κάνουμε deploy custom workers για να έχουν τους πόρους που θέλουμε. Στην περίπτωση αυτή θέλουμε να υλοποιήσουμε όλους τους πόρους άρα η shell εντολή που χρησιμοποιούμε σε κάθε vm είναι 
```bash
spark-daemon.sh start org.apache.spark.deploy.worker.Worker [worker id] --webui-port 8080 --port 65510 --cores 2 --memory 4g spark://[local master ip]:7077
```
με `--port` οτιδήποτε μεταξύ 1024 - 65535.
Για να απενεργοποιήσουμε τους worker εκτελούμε την εντολή 
```bash
spark-daemon.sh stop org.apache.spark.deploy.worker.Worker [worker id]
```

## Εκτέλεση 

Για να εκτελέσουμε προγράμματα πρέπει να εκκινήσουμε αρχικά το cluster με 
```bash
start-dfs.sh
start-master.sh
```
και κάνουμε deploy τους workers με βάση τις ανάγκες μας.

Τελικά, με την shell command 
```bash
spark-submit [path to python file]
```
μπορούμε να εκτελέσουμε python αρχεία.
Αν θέλαμε να εκτελέσουμε μικρά προγράμματα θα μπορούσαμε να δουλέψουμε σε spark shell με την εντολή `pyspark` που είναι πιο γρήγορη διαδικασία από το να γίνεται submit το αρχείο για κάθε μίκρο αλλαγή.

Τα αρχεία του hdfs system μπορούν να φανούν στο UI του hadoop στην σελίδα `http://[master public ipv4]:9870/` ή με hdfs εντολές. Στην σελίδα `http://[public ip master]:8080/` φαίνονται αναλυτικά οι δράσεις του spark.
