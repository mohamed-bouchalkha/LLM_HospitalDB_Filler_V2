# 🏥 Données Hospitalières Maroc - Pipeline ETL

Ce projet est un pipeline complet d'extraction, de transformation et de chargement (ETL) conçu pour centraliser les données sur le système de santé marocain. Il automatise la collecte d'informations sur les hôpitaux, les médicaments, les dispositifs médicaux et les fournisseurs, enrichit ces données via des LLM (Large Language Models) et les structure dans une base de données relationnelle MySQL.

## 🚀 Fonctionnalités

  * **Scraping Multi-Sources** : Collecte automatique depuis des sources gouvernementales et OpenStreetMap.
  * **Normalisation** : Nettoyage des données (standardisation des noms de villes, formats, etc.).
  * **Enrichissement IA** : Utilisation de LLM pour compléter les données manquantes et créer des liens logiques.
  * **Base de Données Relationnelle** : Schéma SQL optimisé pour lier hôpitaux, équipements, services et médicaments.

## 📂 Structure du Projet

```bash
.
├── data/
│   ├── raw/             # Données brutes (JSON, CSV, Excel) issues du scraping
│   ├── processed/       # Données nettoyées et normalisées
│   ├── enriched/        # Données finales enrichies prêtes pour l'import
│   └── cache/           # Cache pour les recherches web et enrichissements
├── scripts/
│   ├── utils/           # Utilitaires (DB loaders, helpers)
│   ├── 1_scraper_complet.py    # Étape 1 : Extraction
│   ├── 2_normalisation.py      # Étape 2 : Nettoyage
│   ├── 3_enrichissement_llm.py # Étape 3 : IA & Enrichissement
│   ├── 4_import_mysql.py       # Étape 4 : Chargement en base
│   └── run_pipeline.py         # Point d'entrée principal (Orchestrateur)
├── mysql_schema.sql     # Schéma de la base de données
└── README.md            # Documentation du projet
```

## ⚙️ Prérequis

  * **Python 3.10+**
  * **MySQL Server** (local ou distant)
  * Bibliothèques Python (liste indicative basée sur les scripts) :
      * `pandas`
      * `mysql-connector-python`
      * `requests`, `beautifulsoup4` (pour le scraping)
      * `openai` ou autre client LLM (pour l'enrichissement)

## 🛠️ Installation

1.  **Cloner le dépôt :**

    ```bash
    git clone https://github.com/mohamed-bouchalkha/LLM_HospitalDB_Filler_V2
    ```

2.  **Configurer l'environnement virtuel :**

    ```bash
    python -m venv venv
    source venv/bin/activate  # Sur Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  **Préparer la base de données :**
    Vous pouvez créer la base de données manuellement ou laisser le script d'import s'en charger, mais assurez-vous que votre serveur MySQL est lancé.

## ▶️ Utilisation

Le projet dispose d'un orchestrateur central qui exécute les 4 étapes séquentiellement.

Lancez simplement :

```bash
python scripts/run_pipeline.py
```

Le script vous guidera à travers les étapes et vous demandera vos identifiants MySQL à l'étape 4 :

  * **Étape 1 : Scraping** (Peuple `data/raw/`)
  * **Étape 2 : Normalisation** (Peuple `data/processed/`)
  * **Étape 3 : Enrichissement** (Peuple `data/enriched/`)
  * **Étape 4 : Import MySQL** (Crée les tables et insère les données)

> **Note :** Le script interactif vous demandera l'hôte, l'utilisateur, le mot de passe et le nom de la base de données (par défaut `morocco_hospitals`) avant l'import.

## 🗄️ Modèle de Données (MySQL)

Le schéma (`mysql_schema.sql`) est conçu pour maintenir une forte intégrité référentielle :

### Entités Principales

  * **`places`** : Hiérarchie géographique (Région \> Province \> Ville).
  * **`hospitals`** : Établissements de santé (Publics, Cliniques, CHU).
  * **`suppliers`** : Fournisseurs de matériel et grossistes pharmaceutiques.
  * **`medications`** : Base de données des médicaments (DCI, Dosage, Prix, Fabricant).
  * **`equipment`** : Référentiel des équipements médicaux.
  * **`services`** : Liste des services médicaux (Cardiologie, Urgences, etc.).

### Relations (Tables de liaison)

  * `hospital_services` : Quels services sont disponibles dans quel hôpital.
  * `hospital_equipment` : Inventaire des équipements par hôpital.
  * `hospital_medications` : Stock/Disponibilité des médicaments par hôpital.
  * `supplier_medications` & `supplier_equipment` : Catalogues des fournisseurs.

## 📝 Logs et Monitoring

  * Les logs d'exécution sont affichés dans la console avec un formatage clair.
  * En cas d'erreur, le pipeline s'arrête et affiche la trace pour le débogage.
  * Les données intermédiaires sont sauvegardées à chaque étape dans le dossier `data/` pour vérification manuelle si nécessaire.
