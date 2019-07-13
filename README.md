# FlatScraper

## Prérequis
Docker

## Lancer l'application
#### Via Docker
Remplir le env_file.tmpl
```bash
$ docker build -t flat_scraper .
$ docker run --env-file=env_file.tmpl flat_scraper
```
