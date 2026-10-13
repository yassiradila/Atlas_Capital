# Atlas Capital — Guide d'Étude & d'Architecture Complète

> **Objectif de ce guide** : Ce document a été conçu pour vous donner une maîtrise totale et intuitive de l'ensemble du projet **Atlas Capital**. Il détaille l'arborescence, chaque fichier, chaque composant d'infrastructure (Docker, Selenium, PostgreSQL, Airflow, dbt) et chaque flux de données. À la fin de cette lecture, vous serez capable d'expliquer et de défendre chaque décision technique en entretien d'embauche, comme si vous aviez conçu chaque ligne vous-même.

---

## Sommaire

1. [La Vue d'Ensemble & Le Flux de Données](#1-la-vue-densemble--le-flux-de-données)
2. [Arborescence Détaillée du Projet : Rôle de Chaque Fichier](#2-arborescence-détaillée-du-projet--rôle-de-chaque-fichier)
3. [L'Infrastructure Docker & Les Volumes : Comment Tourne le Projet](#3-linfrastructure-docker--les-volumes--comment-tourne-le-projet)
4. [Étape 1 : L'Extraction Web & Selenium (Scraping Headless)](#4-étape-1--lextraction-web--selenium-scraping-headless)
5. [Étape 2 : Le Stockage Brut (Fichiers Partitionnés sur Disque)](#5-étape-2--le-stockage-brut-fichiers-partitionnés-sur-disque)
6. [Étape 3 : La Couche Bronze (Ingestion Idempotente & Protocole COPY)](#6-étape-3--la-couche-bronze-ingestion-idempotente--protocole-copy)
7. [Étape 4 : La Couche Silver & dbt (Nettoyage, Typage & Déduplication)](#7-étape-4--la-couche-silver--dbt-nettoyage-typage--déduplication)
8. [Étape 5 : La Couche Gold & Modélisation Dimensionnelle (Star Schema)](#8-étape-5--la-couche-gold--modélisation-dimensionnelle-star-schema)
9. [L'Orchestration avec Apache Airflow (DAGs, Calendrier & Date Logique)](#9-lorchestration-avec-apache-airflow-dags-calendrier--date-logique)
10. [Les Concepts Fondamentaux du Data Engineering (Analogies & Théorie)](#10-les-concepts-fondamentaux-du-data-engineering-analogies--théorie)
11. [Questions Clés d'Entretien & Réponses Types](#11-questions-clés-dentretien--réponses-types)
12. [Par Quoi Commencer ? Les Fichiers Clés à Étudier en Priorité](#12-par-quoi-commencer--les-fichiers-clés-à-étudier-en-priorité)

---

## 1. La Vue d'Ensemble & Le Flux de Données

Atlas Capital est un pipeline de données de bout en bout (**ELT** automatisé). Son rôle est d'extraire quotidiennement les cotations boursières des sociétés cotées à la **Bourse de Casablanca** depuis le portail financier *Le Boursier* (Media24), de stocker ces données brutes sans altération, puis de les transformer au sein d'un entrepôt de données PostgreSQL structuré en **architecture médaillon** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) prêt pour la Business Intelligence (Power BI).

### Schéma du Flux de Données

```
┌────────────────────────────────────────────────────────┐
│   Portail Web : Le Boursier (medias24.com/leboursier)   │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ 1. Pilote Chrome distant (Selenium Webdriver HTTP)
                            ▼
┌────────────────────────────────────────────────────────┐
│  Scraper Python (src/atlas/scraping/scraper.py)        │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ 2. Écriture de fichiers CSV partitionnés (Immutable)
                            ▼
┌────────────────────────────────────────────────────────┐
│  Lake Brut : data/raw/le_boursier/trade_date=YYYY-MM-DD │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ 3. Ingestion PostgreSQL (DELETE partition + STREAMING COPY)
                            ▼
┌────────────────────────────────────────────────────────┐
│  PostgreSQL Bronze : bronze.le_boursier_quotes         │
│  (Colonnes en TEXT brut, traçabilité et audit)         │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ 4. Transformation dbt (Nettoyage regex, typage, déduplication)
                            ▼
┌────────────────────────────────────────────────────────┐
│  PostgreSQL Silver : silver.stg_le_boursier_quotes     │
│  (Types stricts NUMERIC/DATE/BIGINT, table incrémentale)│
└───────────────────────────┬────────────────────────────┘
                            │
                            │ 5. Modélisation Dimensionnelle dbt (Star Schema)
                            ▼
┌────────────────────────────────────────────────────────┐
│  PostgreSQL Gold (Schéma en Étoile pour l'analyse)     │
│  ├── dim_security      (Clés de substitution INT)     │
│  ├── dim_date          (Clés de date YYYYMMDD, fériés) │
│  ├── fact_daily_quote  (Faits de cotation + LAG veille)│
│  └── fact_quote_metrics(Vue : Moyennes mobiles 20/50j) │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Consommateurs : Power BI Desktop / Analystes SQL      │
└────────────────────────────────────────────────────────┘
```

---

## 2. Arborescence Détaillée du Projet : Rôle de Chaque Fichier

Voici la structure exacte du dépôt et l'explication précise du rôle de chaque dossier et fichier :

```
atlascapitalv2/
├── .github/
│   └── workflows/
│       └── ci.yml                  # Intégration continue (GitHub Actions)
├── dags/                           # Dossier des pipelines orchestrés par Airflow
│   ├── market_daily_pipeline.py    # DAG quotidien automatique (18h00, jours ouvrés)
│   └── market_backfill.py          # DAG de rattrapage historique paramétrable
├── data/
│   └── raw/                        # Fichiers bruts partitionnés (data lake local)
│       └── le_boursier/
│           └── trade_date=YYYY-MM-DD/
│               └── quotes.csv      # Données brutes telles que reçues du web
├── dbt/                            # Projet dbt (Data Build Tool)
│   ├── dbt_project.yml             # Configuration globale du projet dbt
│   ├── packages.yml                # Dépendances externes (ex: dbt-utils)
│   ├── profiles.yml                # Configuration de connexion à la base de données
│   ├── macros/
│   │   └── generate_schema_name.sql# Contrôle le nommage exact des schémas (silver, gold)
│   └── models/
│       ├── sources.yml             # Déclaration des tables sources (schéma bronze)
│       ├── silver/
│       │   ├── schema.yml          # Documentation & tests de qualité pour le Silver
│       │   └── stg_le_boursier_quotes.sql # Modèle SQL nettoyant et typant la table brute
│       └── gold/
│           ├── schema.yml          # Tests d'intégrité référentielle (clés étrangères)
│           ├── dim_date.sql        # Dimension Date (calendrier et jours ouvrés)
│           ├── dim_security.sql    # Dimension Titre (entreprises cotées)
│           ├── fact_daily_quote.sql# Table de faits (cours, variations, LAG veille)
│           └── fact_quote_metrics.sql # Vue analytique (moyennes mobiles MA20, MA50)
├── docker/                         # Fichiers de construction des conteneurs
│   ├── airflow/
│   │   ├── Dockerfile              # Image Airflow personnalisée (avec Python 3.11, dbt, psycopg3)
│   │   └── requirements.txt        # Librairies Python installées dans l'image Airflow
│   └── postgres/
│       └── init.sql                # Script SQL exécuté à la première initialisation de PostgreSQL
├── docs/                           # Documentation technique
│   ├── architecture.txt            # Spécification d'architecture originelle
│   └── PROJECT_GUIDE.md            # Ce guide d'étude complet
├── logs/                           # Fichiers de logs générés par Airflow et dbt
├── src/                            # Code source applicatif Python
│   └── atlas/
│       ├── loading/
│       │   └── bronze_loader.py    # Chargeur idempotent des fichiers CSV vers Postgres Bronze
│       ├── scraping/
│       │   ├── companies.txt       # Liste des entreprises à extraire (config texte)
│       │   ├── driver.py           # Usine créant la connexion distante au navigateur Chrome
│       │   ├── parser.py           # Fonctions pures de parsing (dates, nombres français, regex)
│       │   └── scraper.py          # Logique d'extraction web pilotant Selenium
│       └── utils/
│           ├── calendar.py         # Gestion du calendrier marocain (week-ends & jours fériés)
│           └── db.py               # Gestionnaire de connexion à la base de données (psycopg3)
├── tests/                          # Tests unitaires et d'intégration (Pytest)
│   ├── test_bronze_loader.py       # Teste le chargement atomique et l'idempotence
│   ├── test_dag_integrity.py       # Teste la validité syntaxique des DAGs Airflow
│   ├── test_extraction.py          # Teste la logique du scraper et la gestion des dates
│   └── test_parsing.py             # Teste le parsing des nombres français et des dates
├── .env.example                    # Exemple de variables d'environnement
├── docker-compose.yml              # Orchestration multi-conteneurs locale
├── Makefile                        # Raccourcis de commandes terminal (make test, make up, etc.)
└── pyproject.toml                  # Configuration du formateur/linter Ruff et de Pytest
```

---

## 3. L'Infrastructure Docker & Les Volumes : Comment Tourne le Projet

Dans ce projet, rien n'est installé "en vrac" sur la machine hôte. Tout s'exécute dans des conteneurs isolés gérés par **Docker Compose** ([`docker-compose.yml`](file:///c:/Users/PC/Desktop/atlascapitalv2/docker-compose.yml)).

### Qu'est-ce qu'un Conteneur Docker ?
Un conteneur est comme une machine virtuelle ultra-légère. Au lieu d'émuler tout un ordinateur, il partage le noyau du système d'exploitation hôte mais encapsule ses propres programmes, bibliothèques et configurations. Cela garantit que le projet fonctionne **exactement de la même manière** sur votre PC Windows, sur le Mac d'un collègue, ou sur un serveur Linux en production.

### Quels sont les 4 Services Actifs ?
1. **`postgres` (`atlas_postgres`)** :
   - Image officielle `postgres:15-alpine`.
   - Il héberge deux bases de données distinctes :
     - `airflow` : pour le stockage interne d'Airflow (historique des tâches, états, verrous).
     - `warehouse` : notre entrepôt de données métier (schémas `bronze`, `silver`, `gold`).
2. **`selenium` (`atlas_selenium`)** :
   - Image `selenium/standalone-chrome:latest`.
   - C'est un conteneur dédié qui fait tourner Google Chrome.
   - Il expose le port `4444` (API HTTP WebDriver) et le port `7900` (noVNC pour voir l'écran du navigateur en direct via une interface web).
3. **`airflow-scheduler` (`atlas_airflow_scheduler`)** :
   - Construit à partir de notre [`docker/airflow/Dockerfile`](file:///c:/Users/PC/Desktop/atlascapitalv2/docker/airflow/Dockerfile).
   - C'est le cerveau qui surveille l'heure, lit les DAGs et déclenche les tâches au bon moment.
4. **`airflow-webserver` (`atlas_airflow_webserver`)** :
   - L'interface graphique web d'Airflow, accessible sur `http://localhost:8080`.

### Comment Communiquent les Conteneurs ?
Docker Compose crée un réseau privé virtuel nommé `atlas_network`.
À l'intérieur de ce réseau, les conteneurs se parlent en utilisant **le nom de leur service** comme adresse IP :
- Pour contacter PostgreSQL, le code Python utilise l'hôte `postgres:5432`.
- Pour contacter Chrome, le scraper utilise `http://selenium:4444/wd/hub`.

### Comment Fonctionnent les Volumes Docker ? Où Sont Stockées les Données ?
Par défaut, si un conteneur s'arrête ou est supprimé, toutes les données écrites à l'intérieur disparaissent. C'est ce qu'on appelle la nature *éphémère* des conteneurs. Pour conserver les données, on utilise des **volumes** :

1. **Volume nommé (`named volume`) pour PostgreSQL** :
   ```yaml
   volumes:
     - postgres_data:/var/lib/postgresql/data
   ```
   Docker réserve un espace disque persistant sur votre disque dur. Si vous redémarrez votre machine ou faites `docker compose down`, votre base de données n'est **jamais perdue**. Elle est restaurée dès que le conteneur redémarre.
2. **Montage lié (`bind mount`) pour le code et les fichiers bruts** :
   ```yaml
   volumes:
     - ./dags:/opt/airflow/dags
     - ./src:/opt/airflow/src
     - ./dbt:/opt/airflow/dbt
     - ./data/raw:/opt/airflow/data/raw
   ```
   Un "bind mount" est un miroir direct entre un dossier de votre ordinateur (ex: `c:\Users\PC\Desktop\atlascapitalv2\data\raw`) et un dossier dans le conteneur (`/opt/airflow/data/raw`).
   - Quand le scraper Python écrit un fichier CSV dans `/opt/airflow/data/raw/...`, le fichier apparaît **instantanément sur votre vrai disque Windows**.
   - Quand vous modifiez un script Python ou un modèle SQL sur votre éditeur de code, le conteneur voit la modification immédiatement sans avoir besoin de reconstruire l'image Docker !

---

## 4. Étape 1 : L'Extraction Web & Selenium (Scraping Headless)

### Qu'est-ce que Selenium et Pourquoi l'Utiliser ?
Habituellement, pour récupérer des données sur le web, on utilise de simples requêtes HTTP (`requests.get()` ou `curl`).
Cependant, le site *Le Boursier* est une application web moderne :
- Le tableau des cours n'est pas présent dans le code HTML brut de la page.
- Il est chargé dynamiquement par du code JavaScript dans le navigateur après que l'utilisateur a sélectionné une date et cliqué sur "Chercher".
- Un simple `requests.get()` ne renverrait qu'une page vide sans données.

**Selenium** est un outil qui pilote un véritable navigateur web par programmation : il ouvre la page, attend que le JavaScript s'exécute, remplit les champs de formulaire, clique sur les boutons et lit le DOM (la structure de la page affichée).

### Chrome a-t-il été installé dans Python ? (Architecture Standalone)
**Non.** Installer Chrome et ses dépendances graphiques directement dans l'image Airflow alourdirait l'image de plusieurs gigaoctets et créerait des conflits de versions.
Nous avons séparé les responsabilités :
- Un conteneur autonome officiel `selenium/standalone-chrome` fait tourner Chrome.
- Notre code Python dans Airflow utilise `selenium.webdriver.Remote` ([`src/atlas/scraping/driver.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/scraping/driver.py)) : il envoie de simples instructions HTTP légères au conteneur Chrome via le protocole W3C WebDriver sur le port `4444`.

### Qu'est-ce qu'un Navigateur "Headless" ?
Un navigateur "headless" (sans tête) est une instance normale de Google Chrome, mais qui s'exécute **sans ouvrir de fenêtre graphique à l'écran**.
- Il consomme 80% moins de mémoire vive (RAM) et de processeur (CPU).
- Il peut tourner parfaitement sur un serveur dans le cloud qui ne possède aucun écran ni bureau graphique.
- *Astuce noVNC* : Le conteneur Selenium intègre aussi un serveur VNC sur le port `7900`. Si vous voulez voir Chrome naviguer en direct avec vos yeux pour déboguer, il suffit d'ouvrir `http://localhost:7900` dans votre navigateur (mot de passe : `secret`).

### Comment le Scraper Évite-t-il le Blocage Anti-Bot ?
Les sites web détectent souvent les navigateurs automatisés grâce à la variable JavaScript `window.navigator.webdriver` qui vaut `true` par défaut sous Selenium.
Dans [`src/atlas/scraping/driver.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/scraping/driver.py#L42-L46), nous utilisons une commande CDP (*Chrome DevTools Protocol*) pour neutraliser ce drapeau avant même que la page ne se charge :
```python
driver.execute_cdp_cmd(
    "Page.addScriptToEvaluateOnNewDocument",
    {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
)
```
Le serveur web distant pense ainsi avoir affaire à un utilisateur humain utilisant Google Chrome sur Windows.

### Comment Fonctionne l'Extraction dans [`scraper.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/scraping/scraper.py) ?
1. Le scraper lit la liste des entreprises dans [`companies.txt`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/scraping/companies.txt) (ex: `Attijariwafa Bank`, `Maroc Telecom`, `BCP`...).
2. Pour chaque société, il génère l'URL (ex: `https://medias24.com/leboursier/fiche-action/attijariwafa-bank/`).
3. Il injecte les dates de début et de fin directement dans les champs `<input type="date">` via du code JavaScript (`driver.execute_script(...)`), ce qui est beaucoup plus rapide et stable que de simuler des frappes au clavier.
4. Il clique sur le bouton "Chercher" et attend que le tableau se rafraîchisse.
5. Il extrait les lignes HTML du tableau et envoie les chaînes de texte au module de parsing.

---

## 5. Étape 2 : Le Stockage Brut (Fichiers Partitionnés sur Disque)

Une fois les données extraites par Selenium, nous ne les envoyons pas directement dans la base de données finale. Nous les enregistrons sous forme de fichiers CSV dans un format de répertoire standardisé :
`data/raw/le_boursier/trade_date=YYYY-MM-DD/quotes.csv`

### Pourquoi ce Format de Dossier `trade_date=YYYY-MM-DD` ?
Ce format s'appelle le **partitionnement de style Hive** (*Hive-style partitioning*). C'est la convention universelle utilisée dans les Data Lakes modernes (AWS S3, Google Cloud Storage, Databricks, Snowflake).

### Les 3 Avantages Majeurs :
1. **Élagage de partition (*Partition Pruning*)** : Si nous devons retraiter uniquement la journée du 2 octobre 2026, nous n'avons pas besoin de lire un gigantesque fichier de 500 Mo contenant 10 ans d'historique. Nous lisons directement le dossier `trade_date=2026-10-02/`.
2. **Immuabilité & Sécurité** : Les fichiers bruts sont écrits une seule fois et ne sont **jamais modifiés**. Si notre base de données PostgreSQL subit une panne matérielle ou si un stagiaire supprime une table par erreur, l'intégralité de l'historique financier est intacte sur le disque.
3. **Traçabilité (Audit)** : Chaque ligne CSV enregistrée contient un champ `extracted_at` avec l'horodatage UTC exact du moment où la donnée a été extraite du web.

---

## 6. Étape 3 : La Couche Bronze (Ingestion Idempotente & Protocole COPY)

La couche Bronze est la porte d'entrée dans la base de données PostgreSQL ([`bronze.le_boursier_quotes`](file:///c:/Users/PC/Desktop/atlascapitalv2/docker/postgres/init.sql#L31-L44)).

### Pourquoi Toutes les Colonnes Sont-elles en `TEXT` ?
Dans la table Bronze :
- `close_price` est un `TEXT` (pas un `NUMERIC`).
- `volume` est un `TEXT` (pas un `BIGINT`).
- `variation_pct` est un `TEXT`.

**Pourquoi ?** Parce qu'une source externe peut changer de comportement sans préavis. Si demain le site web affiche `"N/A"`, `"-"`, ou `"120,50 MAD"` au lieu d'un nombre propre, une base de données avec des types stricts refuserait la ligne et **ferait planter tout le pipeline de nuit**. En stockant tout en `TEXT`, l'ingestion réussit toujours à 100%. Le travail de nettoyage et de validation est délégué aux couches suivantes.

### Le Protocole `COPY` vs `INSERT` : Pourquoi est-ce Important ?
La plupart des développeurs débutants écrivent une boucle qui exécute `INSERT INTO bronze ...` pour chaque ligne.
- Pour 10 000 lignes, cela représente 10 000 allers-retours réseau entre Python et la base de données.
- PostgreSQL doit analyser, compiler et valider la syntaxe SQL 10 000 fois. C'est très lent.

Dans [`src/atlas/loading/bronze_loader.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/loading/bronze_loader.py#L73-L96), nous utilisons le protocole natif **PostgreSQL `COPY`** via `psycopg 3` (`cur.copy()`) :
- Il ouvre un flux binaire direct vers le disque de stockage de la table.
- Les données sont transmises sous forme de flux continu sans passer par l'analyseur SQL.
- Il est **10 à 50 fois plus rapide** qu'un `INSERT` classique.

### La Garantie d'Idempotence (DELETE + COPY dans une Transaction)
Si Airflow relance une tâche d'ingestion qui a échoué à mi-chemin, comment éviter d'avoir des doublons ?
Dans `bronze_loader.py`, nous exécutons deux actions dans une **seule transaction atomique** :
1. `DELETE FROM bronze.le_boursier_quotes WHERE trade_date = %s;`
2. `COPY bronze.le_boursier_quotes ... FROM STDIN;`

Si l'opération réussit, la nouvelle journée remplace l'ancienne proprement. Si le réseau coupe au milieu du streaming, PostgreSQL annule tout (`ROLLBACK`) : aucune ligne n'est supprimée et aucune ligne partielle n'est ajoutée.

---

## 7. Étape 4 : La Couche Silver & dbt (Nettoyage, Typage & Déduplication)

### Qu'est-ce que dbt (Data Build Tool) ?
dbt est l'outil standard de l'industrie pour la phase **Transformation** du paradigme ELT.
- dbt ne déplace pas les données. Il se connecte à PostgreSQL et exécute des requêtes SQL déclaratives.
- Vous écrivez simplement des requêtes `SELECT`. dbt s'occupe de créer les tables (`CREATE TABLE AS SELECT`), de gérer les dépendances entre les modèles et d'exécuter des tests de qualité automatisés.

### Les Fichiers Clés de dbt dans ce Projet :
- [`dbt/dbt_project.yml`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/dbt_project.yml) : Définit le nom du projet, les dossiers, et applique les stratégies par défaut (ex: matérialisation incrémentale pour le Silver, tables pour le Gold).
- [`dbt/profiles.yml`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/profiles.yml) : Contient les identifiants de connexion à PostgreSQL (hôte, port, utilisateur, mot de passe lus depuis les variables d'environnement).
- [`dbt/models/sources.yml`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/sources.yml) : Déclare les tables brutes qui existent déjà dans PostgreSQL (notre table `bronze.le_boursier_quotes`), permettant d'utiliser la macro Jinja `{{ source('bronze', 'le_boursier_quotes') }}`.
- [`dbt/models/silver/stg_le_boursier_quotes.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/silver/stg_le_boursier_quotes.sql) : Le modèle de transformation Silver.

### Le Nettoyage des Nombres Français
Les sites francophones comme *Le Boursier* utilisent des formats incompatibles avec les bases de données SQL :
- Des virgules au lieu de points décimaux (`120,50`).
- Des espaces insécables (`\u00a0` ou `\u202f`) comme séparateurs de milliers (`1 500,00`).
- Des signes de pourcentage (`+1,25 %`).

Dans `stg_le_boursier_quotes.sql`, nous utilisons des expressions régulières SQL (*regex*) pour purifier chaque chaîne avant de la convertir :
```sql
NULLIF(
    REGEXP_REPLACE(REPLACE(close_price, ',', '.'), '[^0-9.]', '', 'g'),
    ''
)::NUMERIC(12, 2) AS close_price
```
Cette instruction :
1. Remplace la virgule `,` par un point `.`.
2. Supprime tout caractère qui n'est ni un chiffre ni un point (`[^0-9.]`).
3. Transforme une chaîne vide en `NULL` (`NULLIF`).
4. Convertit le résultat propre en nombre décimal exact `NUMERIC(12, 2)`.

### La Déduplication par Fenêtrage SQL (`ROW_NUMBER`)
Si un même cours d'une entreprise a été ingéré plusieurs fois pour la même date, comment ne garder que le plus récent ?
Nous utilisons la fonction de fenêtrage `ROW_NUMBER()` :
```sql
ROW_NUMBER() OVER (
    PARTITION BY TRIM(LOWER(ticker)), trade_date
    ORDER BY _ingested_at DESC
) AS row_num
...
WHERE row_num = 1
```
Pour chaque couple `(ticker, trade_date)`, PostgreSQL numérote les lignes de la plus récente à la plus ancienne selon l'heure d'ingestion `_ingested_at`. La clause finale ne conserve que la ligne numéro 1.

---

## 8. Étape 5 : La Couche Gold & Modélisation Dimensionnelle (Star Schema)

La couche Gold est la vitrine finale destinée aux analystes financiers et aux tableaux de bord Power BI. Elle est modélisée selon la méthodologie de **Ralph Kimball** : le **Schéma en Étoile** (*Star Schema*).

### Pourquoi un Schéma en Étoile ?
Plutôt que d'avoir une seule table géante de 50 colonnes où l'on répète le nom complet de l'entreprise, son secteur et son pays sur chaque ligne quotidienne, on découpe les données en deux catégories de tables :
1. **Les Dimensions** : Le contexte (Qui ? Quand ? Quoi ?).
2. **Les Faits** : Les événements numériques mesurables (Prix, Volumes, Variations).

```
                     ┌─────────────────────────┐
                     │      dim_security       │
                     │─────────────────────────│
                     │ PK: security_key (INT)  │
                     │     ticker (TEXT)       │
                     │     company_name (TEXT) │
                     └────────────┬────────────┘
                                  │ 1
                                  │
                                  │ N
┌─────────────────────────┐  N ┌──┴────────────────────────┐
│        dim_date         ├────┤     fact_daily_quote      │
│─────────────────────────│  1 │───────────────────────────│
│ PK: date_key (INT)      │    │ FK: security_key (INT)    │
│     date (DATE)         │    │ FK: date_key (INT)        │
│     is_weekend (BOOL)   │    │     close_price (NUMERIC) │
│     is_trading_day(BOOL)│    │     volume (BIGINT)       │
└─────────────────────────┘    │     previous_close (NUM)  │
                               └─────────────┬─────────────┘
                                             │
                                             │ Vue analytique (calcul dynamique)
                                             ▼
                               ┌───────────────────────────┐
                               │     fact_quote_metrics    │
                               │───────────────────────────│
                               │     ma_20 (Moyenne 20j)   │
                               │     ma_50 (Moyenne 50j)   │
                               │     avg_volume_20 (Vol 20j)│
                               └───────────────────────────┘
```

### Les 4 Modèles de la Couche Gold :
1. **[`dim_security.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/gold/dim_security.sql)** :
   Génère une clé de substitution entière artificielle (`security_key`) pour chaque société grâce à `DENSE_RANK() OVER (ORDER BY ticker)`.
2. **[`dim_date.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/gold/dim_date.sql)** :
   Génère un calendrier complet avec des clés entières sous format `YYYYMMDD` (ex: `20261002`), enrichi d'attributs calendaires (`day_name`, `month_name`, `is_weekend`, `is_trading_day`).
3. **[`fact_daily_quote.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/gold/fact_daily_quote.sql)** :
   Joint les cotations nettoyées de la couche Silver avec les dimensions pour remplacer les chaînes de texte par les clés étrangères entières (`security_key`, `date_key`). Il calcule aussi le cours de clôture de la veille via la fonction `LAG()` :
   ```sql
   LAG(close_price, 1) OVER (
       PARTITION BY security_key
       ORDER BY trade_date ASC
   ) AS previous_close_price
   ```
4. **[`fact_quote_metrics.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/gold/fact_quote_metrics.sql)** :
   Ce modèle est configuré comme une **VUE** (`materialized='view'`). Il calcule à la volée les indicateurs quantitatifs clés pour le trading algorithmique :
   - **Moyenne Mobile à 20 jours (SMA 20)** :
     ```sql
     AVG(close_price) OVER (
         PARTITION BY security_key
         ORDER BY trade_date ASC
         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
     ) AS ma_20
     ```
   - **Moyenne Mobile à 50 jours (SMA 50)**.
   - **Volume moyen glissant sur 20 jours**.

---

## 9. L'Orchestration avec Apache Airflow (DAGs, Calendrier & Date Logique)

Apache Airflow est le chef d'orchestre de la plateforme. Son rôle est de déclencher les tâches dans le bon ordre, de gérer les dépendances et de réessayer automatiquement en cas de défaillance.

### Le DAG Quotidien : [`market_daily_pipeline.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/dags/market_daily_pipeline.py)
Il tourne automatiquement du lundi au vendredi à 18h00 heure de Casablanca (`schedule="0 18 * * 1-5"`) :

```
[ check_market_open ] ──> [ extract_to_raw ] ──> [ load_bronze ] ──> [ validate_bronze ] ──> [ dbt_run ] ──> [ dbt_test ]
```

1. **`check_market_open`** : Vérifie si la Bourse de Casablanca était ouverte ce jour-là ([`src/atlas/utils/calendar.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/utils/calendar.py)). Si c'est un week-end ou un jour férié marocain (ex: Fête du Trône, Aïd el-Fitr), la tâche lève une exception `AirflowSkipException`. Airflow passe alors toutes les tâches suivantes en état **SKIPPED** et le pipeline se termine avec succès sans consommer inutilement de ressources.
2. **`extract_to_raw`** : Lance le scraper Selenium pour la date du jour et enregistre le CSV brut dans `data/raw/`.
3. **`load_bronze`** : Charge le CSV brut de façon idempotente dans la table `bronze.le_boursier_quotes`.
4. **`validate_bronze`** : Teste qu'au moins une ligne a bien été insérée dans la base. Si la table est vide, la tâche échoue immédiatement pour bloquer la suite.
5. **`dbt_run`** : Exécute `dbt run` pour transformer la donnée brute vers le Silver puis le Gold.
6. **`dbt_test`** : Exécute tous les tests de qualité (unicité, non-nullité, intégrité référentielle).

### Le DAG de Rattrapage : [`market_backfill.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/dags/market_backfill.py)
Si vous devez charger 6 mois d'historique boursier passé :
- Il ne s'exécute jamais automatiquement (`schedule=None`).
- Vous le déclenchez manuellement depuis l'interface web d'Airflow en fournissant deux paramètres dans un formulaire : `start_date` et `end_date`.
- Il extrait l'historique de chaque société en une seule requête par société grâce au formulaire de date du site, enregistre tous les CSV de chaque jour, puis les charge tous en base avant de lancer dbt.

### Le Piège de Débutant : Date Réelle (`datetime.now()`) vs Date Logique (`ds`)
Dans un orchestrateur, il est **formellement interdit** d'écrire en Python :
```python
today = datetime.now().strftime("%Y-%m-%d")  # MAUVAISE PRATIQUE
```
**Pourquoi ?**
Imaginez que nous soyons mardi. Le pipeline de lundi a échoué à cause d'une panne réseau chez l'hébergeur. Mardi matin, vous arrivez au bureau et cliquez sur "Relancer" sur la tâche de lundi.
- Si votre code utilise `datetime.now()`, le script va extraire les données de **mardi** au lieu de rattraper **lundi** !
- **La solution** : Airflow injecte une variable nommée **`ds`** (*Logical Date*) dans chaque fonction Python (`def _task_extract_to_raw(ds: str, **context)`).
Si la tâche en cours correspond à la session du 5 octobre, `ds` vaudra toujours `'2026-10-05'`, peu importe l'heure ou le jour où la machine exécute le code. Vos pipelines deviennent reproductibles et permettent de "voyager dans le temps".

---

## 10. Les Concepts Fondamentaux du Data Engineering (Analogies & Théorie)

Voici les concepts théoriques généraux expliqués simplement, avec des analogies concrètes :

### 10.1 ETL vs ELT
- **L'Analogie** :
  - **ETL (Extract - Transform - Load)** : Vous achetez des légumes au supermarché, vous les épluchez et les faites cuire dans votre voiture sur le trajet du retour, puis vous rangez la soupe cuite dans le frigo. Si vous avez mis trop de sel dans la voiture, la soupe est gâchée et vous devez retourner au supermarché racheter des légumes.
  - **ELT (Extract - Load - Transform)** : Vous achetez les légumes, vous les rangez directement crus dans votre frigo (**Extract & Load**). Plus tard, vous les sortez et les cuisinez dans votre cuisine (**Transform**). Si la recette est ratée, les légumes d'origine sont toujours dans le frigo : vous pouvez tester une nouvelle recette sans jamais retourner au supermarché.
- **Pourquoi ELT est meilleur ici** : Nous sauvegardons le web brut dans PostgreSQL Bronze. Si notre règle de calcul de la moyenne mobile change dans 6 mois, nous n'avons pas besoin de rescraper le web : nous modifions juste notre fichier SQL dbt et nous régénérons la table Gold en 3 secondes.

### 10.2 L'Architecture Médaillon
- **Bronze (Brut)** : Le minerai brut extrait de la mine (avec la terre et la roche). On ne filtre rien.
- **Silver (Conforme)** : Le métal fondu et purifié sous forme de lingots standardisés à 99,9%. Les doublons sont éliminés, les formats nettoyés. C'est la source de vérité unique.
- **Gold (Métier)** : Les bijoux finis (montres, bagues) prêts à être vendus en vitrine pour les clients. Tables optimisées pour l'analyse métier directe.

### 10.3 L'Idempotence
- **L'Analogie** : Le bouton d'appel de l'ascenseur. Si vous appuyez une fois, il s'allume et appelle l'ascenseur. Si vous appuyez 15 fois avec énervement, l'ascenseur n'arrive qu'une seule fois. L'état final du système est identique.
- **En Data Engineering** : Une opération est dite **idempotente** si le fait de l'exécuter 1 fois, 2 fois ou 50 fois produit exactement le même résultat dans la base de données :
  $$f(f(x)) = f(x)$$
- Si un script insère aveuglément des lignes (`INSERT INTO`), chaque exécution double les enregistrements. Avec notre mécanisme `DELETE` de la date puis `COPY`, le nombre de lignes final est toujours exactement celui de la journée traitée.

### 10.4 Clé Naturelle vs Clé de Substitution (*Surrogate Key*)
- **Clé Naturelle** : Une valeur qui existe dans le monde réel (ex: le symbole boursier `'ATW'` ou le nom `'Attijariwafa Bank'`).
  - *Problème* : Faire des jointures SQL entre deux tables de plusieurs millions de lignes sur des chaînes de texte est lent pour le processeur, et si une société change de nom commercial, il faut mettre à jour des millions de lignes historiques.
- **Clé de Substitution** : Un entier artificiel généré par l'entrepôt de données (ex: `1`, `2`, `3`...).
  - *Avantage* : Joindre deux tables sur un entier de 4 octets prend un seul cycle processeur (extrêmement rapide) et isole l'entrepôt des changements de noms commerciaux externes.

### 10.5 Fonctions de Fenêtrage (*Window Functions*)
- **`GROUP BY`** : Pose une question à un groupe d'élèves : *"Quelle est la taille moyenne de la classe ?"*. Le professeur répond avec un seul chiffre : "175 cm". Les identités individuelles des élèves disparaissent.
- **Fonction de Fenêtrage (`OVER (...)`)** : Le professeur passe devant chaque élève individuellement et lui dit : *"Par rapport à l'élève assis juste devant toi, tu mesures 3 cm de plus"*. Chaque élève reste présent sur sa propre ligne, mais dispose désormais d'un calcul relatif à ses voisins. C'est ce qui permet de comparer le cours d'aujourd'hui avec celui d'hier (`LAG`) ou de faire une moyenne sur les 20 derniers jours (`ROWS BETWEEN 19 PRECEDING AND CURRENT ROW`).

---

## 11. Questions Clés d'Entretien & Réponses Types

### Q1 : "Pourquoi avoir utilisé Selenium plutôt que Scrapy ou BeautifulSoup ?"
> *"Le portail Le Boursier génère son tableau de cours dynamiquement via JavaScript après soumission d'un formulaire de dates. Une bibliothèque comme BeautifulSoup ou Scrapy ne reçoit que le code HTML initial statique sans les données de cotation. Nous avons donc utilisé Selenium pour piloter une vraie instance de Chrome capable d'exécuter le moteur JavaScript du site."*

### Q2 : "Comment avez-vous géré les performances de Selenium qui est souvent réputé lent ?"
> *"Nous avons appliqué trois optimisations majeures :*
> *1. Chrome s'exécute en mode headless (`--headless=new`) dans un conteneur dédié, sans interface graphique, ce qui réduit la consommation de mémoire de 80%.*
> *2. Pour le rattrapage historique, au lieu de faire 250 requêtes journalières par société, nous injectons directement la plage de dates complète dans le formulaire via JavaScript, ce qui récupère tout l'historique d'une entreprise en un seul chargement de page.*
> *3. Dès que le HTML du tableau est récupéré, le parsing est délégué à du code Python pur sans plus aucune interaction avec le navigateur."*

### Q3 : "Pourquoi utiliser PostgreSQL et pas Snowflake ou BigQuery ?"
> *"La Bourse de Casablanca compte environ 75 sociétés cotées. À raison d'une cotation par jour et par société sur 250 jours de bourse par an, cela représente environ 18 750 lignes par an. C'est un volume très modeste. Mettre en place un entrepôt distribué comme Snowflake ou BigQuery aurait entraîné des coûts d'infrastructure et une complexité inutiles. PostgreSQL gère des millions de lignes sur une seule machine avec d'excellentes performances grâce aux index B-Tree et au protocole `COPY`."*

### Q4 : "Comment garantissez-vous que votre pipeline est idempotent ?"
> *"À chaque étape du pipeline :*
> *1. Au niveau brut, les fichiers sont partitionnés par date (`trade_date=YYYY-MM-DD`). Écrire à nouveau écrase le fichier de cette journée sans toucher aux autres.*
> *2. Au niveau Bronze, nous exécutons un `DELETE` de la partition de date suivi d'un `COPY` à l'intérieur d'une transaction SQL atomique.*
> *3. Au niveau Silver, dbt applique une stratégie incrémentale `delete+insert` basée sur la clé unique `[ticker, trade_date]`. Relancer une tâche 10 fois produit exactement le même état en base."*

### Q5 : "Que se passe-t-il si le pipeline tourne un jour de week-end ou un jour férié ?"
> *"La première tâche du DAG quotidien appelle notre utilitaire de calendrier `verify_market_open(ds)`. Si la date correspond à un samedi, un dimanche ou un jour férié officiel marocain (Aïd, Fête du Trône...), la tâche lève l'exception `AirflowSkipException`. Airflow passe immédiatement l'ensemble des tâches aval en statut SKIPPED. Le pipeline se termine proprement sans erreur et sans tenter de scraper un marché fermé."*

---

## 12. Par Quoi Commencer ? Les Fichiers Clés à Étudier en Priorité

Si vous devez réviser le projet avant un entretien, voici l'ordre d'apprentissage le plus efficace :

1. **[`dbt/models/silver/stg_le_boursier_quotes.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/silver/stg_le_boursier_quotes.sql)** :
   - Observez le nettoyage des nombres avec `REGEXP_REPLACE` et `REPLACE`.
   - Comprenez comment `ROW_NUMBER() OVER (...)` déduplique les cotations pour ne garder que la version la plus récente.
2. **[`src/atlas/loading/bronze_loader.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/src/atlas/loading/bronze_loader.py)** :
   - Regardez comment le `DELETE` et le protocole `cur.copy()` sont encapsulés dans le gestionnaire de contexte `with conn:` pour garantir l'atomicité de la transaction.
3. **[`dbt/models/gold/fact_quote_metrics.sql`](file:///c:/Users/PC/Desktop/atlascapitalv2/dbt/models/gold/fact_quote_metrics.sql)** :
   - Étudiez la syntaxe de la fenêtre glissante `ROWS BETWEEN 19 PRECEDING AND CURRENT ROW` qui calcule la moyenne mobile à 20 jours.
4. **[`dags/market_daily_pipeline.py`](file:///c:/Users/PC/Desktop/atlascapitalv2/dags/market_daily_pipeline.py)** :
   - Comprenez l'enchaînement des opérateurs Airflow et l'usage du paramètre `ds` (*Logical Date*).
5. **[`docker-compose.yml`](file:///c:/Users/PC/Desktop/atlascapitalv2/docker-compose.yml)** :
   - Comprenez le rôle de chaque conteneur et comment les volumes montent vos fichiers locaux dans les conteneurs.
