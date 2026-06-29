## How to Generate a Paimon Format Directory
This directory is generated using the Paimon Java client.
```
cd paimon-writer && mvn clean package && mvn exec:java -Dcheckstyle.skip=true -Dexec.args="{local_warehouse_path}"
```