# Argentic

Argentic est le moteur d’exécution AgSDL destiné à Macro, l’outil de conception, et à Andrologic Brigade, le produit où les équipes font travailler leurs agents. Un autre logiciel doit pouvoir lui confier le même document, recevoir une demande d’approbation, y répondre et suivre le résultat.

Ce dépôt part du commit `7baf70a`, qui contenait uniquement le titre du projet. Le moteur à reprendre existe dans Macro. Ce README sert de plan de transfert jusqu’à la première livraison autonome.

## État des lieux au 17 novembre 2026

La source de l’import est le commit **`82762f8d9e6a775198c90a1736a5d69edadf34d6`** de Macro, vérifié sur son `develop` distant. Les deux manifestes Cargo y portent la version `0.1.0`. Le contrat de langage est `agsdl-0.3.0`, publié au commit `bd9f8a5b16923476fd20ac2650bd1729ee151917`. Ces deux versions évoluent indépendamment. [Source Macro](https://github.com/Andrologic/Macro/tree/82762f8d9e6a775198c90a1736a5d69edadf34d6/crates) · [Spécification AgSDL](https://github.com/Andrologic/AgSDL/tree/v0.3.0)

| Élément vérifié dans Macro | Ce qu’Argentic doit en faire |
| --- | --- |
| `crates/argentic-core` calcule la compilation, le travail et le routage. Son codec conserve les nombres JSON exacts. | Reprendre la crate entière, avec les schémas, modules, licences et notices. Garder ces calculs indépendants du stockage et des processus. |
| `crates/argentic-runtime` possède l’exécution, les admissions, les approbations, les processus et SQLite. Le schéma autonome porte la révision `post-042`. | Reprendre le runtime et ses tests. Le fichier et la mémoire utilisent les mêmes transactions. Macro garde son historique de migrations et ses tables produit. |
| `standalone::Host` expose déjà le cycle de vie. L’exemple `argentic-host` l’utilise avec un objet JSON par ligne sur stdin et stdout. | Proposer cette API aux hôtes Rust et cet exécutable aux autres langages. L’hôte fournit source, workspace et politique approuvée. Le moteur reste propriétaire des effets et de leur arrêt. |
| Macro fournit l’éditeur, les commandes Tauri, la configuration et la capture propres au produit. Brigade dispose d’un projet web. Octan porte le CRM. | Garder les interfaces, comptes, règles métier et autorisations utilisateur côté produit. Pour Brigade, lancer le processus côté serveur. Évaluer les mécanismes de serveur d’Octan si un besoin de coordination entre machines apparaît. |

Le chantier Macro a rapporté 42 tests du cœur, 346 du runtime, les contrôles d’admission, 37 scénarios sur SQLite fichier et mémoire, et une recette réelle sur le même SHA dans Macro et dans l’hôte autonome. Deux agents se sont passé un fichier après approbation de son écriture. Un second essai dans Macro a vérifié Stop pendant l’attente. Les 24 étapes de CI ont passé, dont le contrôle HTTP après correction du chemin local du binaire. Dans chaque mode SQLite, le test de conformité exige 33 réussites dans le périmètre produit et 4 en dehors de ce périmètre.

Le profil autonome est plus étroit que le langage. La recette Codex utilise le binaire officiel 0.167.0. La documentation borne Codex aux profils qualifiés macOS ARM64 et Linux ARM64 et le flux humain autonome à macOS ARM64. Les appels locaux de l’hôte, les autres harnais, le contenu réseau et MCP distant sont indisponibles dans cet hôte. Garder leurs refus explicites. [Contrat et limites de l’hôte](https://github.com/Andrologic/Macro/blob/82762f8d9e6a775198c90a1736a5d69edadf34d6/crates/argentic-runtime/HOST.md)

## Harnais Argentic

Sous Linux, le shell filtré utilise un relais Rust embarqué dans le binaire.
La préparation d’un arbre de 20 000 fichiers et 60 Mo passe de 543 à 250 ms
sur ext4. Le plafond par racine est désormais de 50 000 entrées et 256 Mio.
La CI Linux relève aussi la latence d’un tour scripté et les octets retenus
par l’historique d’une instance au repos. [Mesures et méthode](docs/argentic-harness.md#lot-6)

## Contribution avec GitHub Flow

`main` est la branche de référence. Pour chaque changement, créer une branche
courte à partir de `origin/main` et ouvrir une pull request vers `main`. La CI
teste la pull request et le résultat de la fusion sur `main`. Fusionner une fois
la revue terminée et la CI au vert, puis supprimer la branche de travail.

## Plan pour la première livraison

Exécuter les lots dans l’ordre. Pour chaque lot, partir du dernier lot intégré sur
`main` et commiter après vérification du résultat indiqué. Les profils Terra
proposés correspondent au travail de chaque lot.

| N° | Travail | Résultat attendu avant de passer au lot suivant | Difficulté et profil conseillé |
| --- | --- | --- | --- |
| 1 | Importer la source qualifiée | Exporter depuis le SHA ci-dessus uniquement `crates/argentic-core` et `crates/argentic-runtime` aux mêmes chemins. Garder leurs deux lockfiles, tests, fixtures, licences AGPL et notices AgSDL. Reprendre la licence racine de Macro. Comparer les arbres Git avant de modifier du code. | Faible, copie vérifiable · Terra medium |
| 2 | Rendre les vérifications autonomes | Depuis un clone neuf, compiler et tester les deux crates avec les commandes ci-dessous. Les exécuter en CI sans checkout Macro, Bun, clé fournisseur ni base produit. Tester sur macOS ARM64 pour le parcours humain. Consigner la plateforme et distinguer les tests ignorés. | Moyenne, dépendances natives · Terra medium |
| 3 | Livrer l’entrée d’intégration | Donner à l’exécutable une cible Cargo installable nommée `argentic-host`. Réutiliser le pilote de l’exemple et préserver son initialisation native. Dans un projet client temporaire, tester la dépendance Rust épinglée à une révision et, dans un client non Rust, piloter le binaire par ses deux flux. | Moyenne, compatibilité de deux entrées · Terra medium |
| 4 | Qualifier le candidat | Rejouer les parcours ci-dessous sur le SHA candidat. Mesurer séparément démarrage à froid, préflight, Start et inspection sur une fixture fixe. Garder le temps fournisseur à part. Relever plateforme, mode SQLite, nombre d’essais et médiane pour disposer d’une référence d’optimisation. | Élevée, effets réels et arrêt · Terra high |
| 5 | Vérifier les consommateurs et livrer | Dans un worktree Macro de qualification, remplacer les dépendances locales par la révision candidate, conserver ses migrations et rejouer sa recette. Pour Brigade, vérifier qu’un petit client serveur peut faire l’aller-retour humain via le binaire. Publier les instructions d’installation réellement testées dans ce README. Proposer le tag une fois les deux chemins vérifiés. | Moyenne, compatibilité produit · Terra high |

**Import du lot 1.** Depuis la racine du worktree Argentic, après avoir vérifié que les deux chemins de destination sont libres et que le clone source contient le SHA, lancer la commande suivante. Remplacer le chemin du clone par celui de la machine.

```bash
set -euo pipefail
source_macro=/chemin/vers/Macro
revision=82762f8d9e6a775198c90a1736a5d69edadf34d6
git -C "$source_macro" archive "$revision" LICENSE crates/argentic-core crates/argentic-runtime | tar -x
```

Après l’import, `git rev-parse HEAD:crates/argentic-core HEAD:crates/argentic-runtime` doit donner respectivement `b57f819b79c18c2d26abc6289bacbaf0d2eb8dae` et `26c1869284bf04b991d38ebc1688aeabafa8f476`. Cela permet de distinguer une différence de copie d’une régression introduite ensuite.

**Commandes du lot 2.** Elles reprennent le profil Cargo du dépôt source. Installer les prérequis natifs de la plateforme, dont Swift sur macOS. L’oracle TypeScript reste un contrôle de parité facultatif avec un éditeur explicitement désigné. [Profil CI source](https://github.com/Andrologic/Macro/blob/82762f8d9e6a775198c90a1736a5d69edadf34d6/dev/ci/check-profiles.mjs)

```sh
set -eu
for crate in argentic-core argentic-runtime; do
  manifest="crates/$crate/Cargo.toml"
  cargo test --locked --manifest-path "$manifest" --all-targets -- --test-threads=1
  cargo test --locked --manifest-path "$manifest" --doc
done
manifest=crates/argentic-runtime/Cargo.toml
cargo build --locked --manifest-path "$manifest" --example argentic-host
cargo test --locked --manifest-path "$manifest" --features dev-support --test approval_boundary -- --test-threads=1
```

Le profil CI Linux exécute aussi les tests du moteur activés par `agsdl-engine-qa`.
Pour les exécuter localement, utilisez le profil de test Cargo par défaut.

```sh
cargo test --locked --manifest-path crates/argentic-runtime/Cargo.toml \
  --features agsdl-engine-qa --lib -- --test-threads=1
```

**Recettes des lots 3 à 5.** Utiliser les fixtures `human-only.json`, `write-review.json` et `policy.json` du dossier `crates/argentic-runtime/examples/fixtures`. Le protocole et les formes exactes des messages figurent dans le `HOST.md` importé.

1. Démarrer avec un répertoire de données privé distinct du workspace et le condensat SHA-256 de la politique examinée. Sur macOS ARM64, exécuter le flux humain sans fournisseur. Lire `ready`, appeler `preflight` puis `start`, lire la demande via `humans`, répondre avec son identifiant et sa révision, puis vérifier le résultat par `inspect`. Réutiliser la même clé Start après une réponse perdue et vérifier qu’il existe une seule exécution.
2. Sur un profil Codex qualifié, préparer `sentinel.txt` contenant `before` et un saut de ligne. Vérifier la commande et le fichier accordé avant de répondre. Le second agent doit relire `after` et son saut de ligne. Vérifier le verdict et la fin physique des deux travaux. Fournir le profil à l’exécution et conserver uniquement les résultats expurgés dans les preuves de livraison.
3. Répéter depuis un workspace neuf, appeler `stop` pendant l’attente. Vérifier que le fichier contient encore `before`, que la demande est close et que les processus sont terminés. Après une interruption brutale distincte, rouvrir la base et vérifier que le travail interrompu reste inspectable et qu’aucun effet incertain n’est rejoué.
4. Côté client serveur, conserver ensemble l’identité du run, sa source et la révision de la demande. Tester aussi une réponse périmée et une capacité indisponible. Le client doit recevoir une erreur exploitable et pouvoir encore inspecter le run. Une seule instance de l’hôte possède le répertoire de données à la fois.

La première livraison proposée tient dans deux bibliothèques et un exécutable. Le service HTTP mutualisé, les files distribuées, de nouveaux fournisseurs et un SDK par langage peuvent attendre un besoin observé chez un consommateur. La première mesure de performance sert à choisir où intervenir après cette livraison.
