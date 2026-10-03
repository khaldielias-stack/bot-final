# 🚂 Déployer le bot sur Railway

Railway fait tourner le bot 24/7 sans garder ton PC allumé. Guide complet, étape par étape.

---

## ⚠️ À savoir avant de commencer

**Le système de fichiers de Railway est éphémère** — tout ce que le bot écrit (`config.json`, `seen_items.json`, `bot_state.json`) est perdu à chaque redéploiement, **sauf** si on attache un **Volume persistant**. C'est fait dans ce guide, ne saute pas cette étape.

**Les secrets ne vont jamais dans `config.json` sur Railway** — le bot a été adapté pour lire `discord_webhook_url` et `discord_bot_token` depuis des **variables d'environnement** à la place, pour ne jamais committer de vrai token dans un repo Git.

---

## 📁 Fichiers nécessaires

Dans ton repo (GitHub, GitLab...), tu dois avoir :

```
vinted_bot.py
dashboard.html
requirements.txt
Dockerfile
railway.toml
.dockerignore
```

**Ne commite jamais** `config.json`, `bot_state.json` ou `seen_items.json` s'ils contiennent déjà tes vraies données — ils seront (re)créés automatiquement sur le Volume au premier démarrage. Le `.dockerignore` fourni les exclut déjà de l'image.

---

## 🚀 Déploiement

### 1. Crée un projet Railway

1. [railway.com](https://railway.com) → **New Project**
2. **Deploy from GitHub repo** → sélectionne ton repo
3. Railway détecte le `Dockerfile` et lance le build automatiquement

### 2. Attache un Volume (obligatoire)

1. Dans le service créé → onglet **Settings** → section **Volumes**
2. **Add Volume**
3. Mount path : **`/data`**

Sans ça, `seen_items.json` repart de zéro à chaque déploiement — le bot renotifie tout ce qu'il retrouve, ou pire, perd la mémoire de ce qu'il a déjà envoyé.

### 3. Configure les variables d'environnement

Onglet **Variables** → ajoute :

| Variable | Valeur |
|---|---|
| `DATA_DIR` | `/data` |
| `DISCORD_WEBHOOK_URL` | ton URL de webhook |
| `DISCORD_BOT_TOKEN` | ton token de bot |

`PORT` est injecté automatiquement par Railway — ne le définis pas toi-même.

### 4. Génère un domaine public (pour le dashboard)

1. Onglet **Settings** → section **Networking**
2. **Generate Domain**
3. Railway te donne une URL du style `tonbot.up.railway.app` — c'est ton dashboard, accessible en HTTPS depuis n'importe où

### 5. Déploie

Railway build et démarre automatiquement après chaque push sur ta branche principale. Vérifie les **logs** (onglet **Deployments** → clique sur le déploiement actif) :

```
✓ Config chargée
✓ Dashboard → http://localhost:XXXX
✓ Session Vinted opérationnelle
✓ Contrôle Discord actif : TonBot#1234
```

---

## ⚙️ Configurer tes recherches

Le volume démarre vide : `config.json` est créé avec une recherche d'exemple au premier lancement. Deux façons de configurer tes vraies recherches :

### Option A — Depuis Discord (recommandé)

C'est exactement ce que le contrôle bidirectionnel a été conçu pour faire — pas besoin de toucher aux fichiers sur le serveur :

```
!vinted add Sneakers 42 | url | https://www.vinted.fr/catalog?catalog[]=1242&size_ids[]=208&price_to=120
!vinted every Sneakers 42 45
```

### Option B — Éditer le volume directement

Via la CLI Railway (`railway run bash` ou `railway shell` selon la version), tu peux ouvrir un terminal dans le conteneur et éditer `/data/config.json` à la main, puis redémarrer le service.

---

## 💰 Coût estimé

Railway facture à l'usage réel (CPU/RAM/volume), au-delà d'un crédit inclus dans le plan **Hobby (5$/mois)**. Ce bot est très léger — faible CPU, peu de RAM, volume de quelques Ko pour les fichiers JSON — et devrait rester largement dans le crédit inclus pour un usage personnel. Vérifie quand même l'onglet **Usage** de Railway après quelques jours pour confirmer, les tarifs pouvant évoluer.

---

## 🛠️ Problèmes spécifiques à Railway

| Symptôme | Cause / solution |
|---|---|
| Les annonces déjà vues reviennent après un redéploiement | Le Volume n'est pas attaché, ou pas monté sur `/data` — vérifie `Settings → Volumes` |
| Dashboard inaccessible | Vérifie qu'un domaine a été généré (`Settings → Networking → Generate Domain`), et que tu accèdes bien en `https://` |
| Le bot redémarre en boucle | Regarde les logs du déploiement — souvent un token/webhook manquant ou mal formé dans les Variables |
| `discord_bot_token` vide alors qu'il est dans les Variables | Vérifie l'orthographe exacte : `DISCORD_BOT_TOKEN` (majuscules, underscore) |
| Changement de config.json local sans effet sur Railway | Normal — sur Railway, `config.json` vit sur le Volume, pas dans le code déployé. Utilise `!vinted add`/`enable`/`disable` depuis Discord |

---

## 🔄 Mettre à jour le bot plus tard

Push sur ta branche → Railway redéploie automatiquement. Grâce au Volume, `seen_items.json` et tes recherches survivent au redéploiement. Le bot gère maintenant proprement l'arrêt (`SIGTERM`, envoyé par Railway à chaque redéploiement) : la sauvegarde a lieu avant la coupure, pas de perte de données en théorie — mais le Volume reste ta vraie garantie de persistance.
