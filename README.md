# Import 

Importer zum Übertragen von Messdaten aus der EcoVision API in einen [FROST-Server](https://fraunhoferiosb.github.io/FROST-Server/).

Die Struktur des FROST-Servers eignet sich zur Darstellung im [Masterportal](https://bitbucket.org/geowerkstatt-hamburg/masterportal/src/dev/) mit dem GFI-Theme [SensorChart](https://github.com/digitale-plattform-stadtverkehr-berlin/masterportal-addon-sensor-chart)

## Parameter

Einige Zugangsdaten müssen über Systemvariablen konfiguriert werden.
Wenn der Service als Docker-Container läuft können diese als Umgebungsvariablen in Docker gesetzt werden.

* **API_URL** - Domain der Eco-Counter API.
* **API_KEY/API_SECRET** - Zugangsdaten für die Eco-Counter API.

* **FROST_SERVER** - Basis Url für den Frost-Server.
* **FROST_USER/FROST_PASSWORD** - Zugangsdaten für den Frost-Server.

## Docker Image bauen und in GitHub Registry pushen

```bash
> docker build -t docker.pkg.github.com/digitale-plattform-stadtverkehr-berlin/eco-counter/eco-counter:<TAG> .
> docker push docker.pkg.github.com/digitale-plattform-stadtverkehr-berlin/eco-counter/eco-counter:<TAG>
```
