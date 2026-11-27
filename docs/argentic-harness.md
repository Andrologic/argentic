# argentic-harness

`argentic-harness` sera le harnais par défaut d’Argentic : le logiciel qui fait
tourner la boucle d’un agent. Il doit appliquer AgSDL `agsdl-0.4.0` exactement,
déclarer `native` chaque propriété et appliquer chaque permission sans
approximation. Il doit aussi rester très configurable et rapide. Aujourd’hui,
Argentic n’exécute que Codex. Les modules Claude Code, opencode et pi y sont
seulement descriptifs.

## Origine

Macro avait un harnais propre, l’« agent résident », décrit par
`harnesses/macro/module.json` dans AgSDL. AgSDL l’a retiré au commit `bfc859a`
du 4 novembre 2026. Macro a supprimé son code au commit `5a110a6e`
(« retire legacy 0.2 execution and schema ») du 12 novembre 2026. Ce harnais
n’a jamais été porté sur le moteur 0.3 (`AgentExecutor030`). Le code se relit
avec `git -C Macro show 5a110a6e^:src/services/agsdlExecution/residentAgent.ts`.

Ses défauts expliquent pourquoi on le refait plutôt que de le restaurer :

- La boucle tournait en TypeScript dans une webview cachée. Elle ne pouvait
  donc pas tourner sans interface.
- Les règles `ask` et `deny` du document n’étaient pas appliquées : le code
  forçait `nativeDecision = 'allow'`, et un niveau de risque propre à Macro
  remplaçait les règles authored.
- Il n’y avait ni steer, ni MCP, ni skills, ni contenu par URI, ni compaction.
  Chaque agent n’avait qu’une instance par exécution.
- La relance sans effort de raisonnement, annoncée par le module, était
  désactivée dans le chemin résident.

## Décisions

| Sujet | Décision |
| --- | --- |
| Processus | Une crate Rust appelée dans le processus du moteur par défaut. Le même protocole est aussi exposé par un binaire stdio pour isoler le harnais ou servir d’autres hôtes. Shell et exécution tournent toujours dans un sous-processus sous sandbox. |
| Fournisseurs v1 | Chat Completions (ollama, lmstudio, macro-ai, OpenRouter…), Anthropic Messages, OpenAI Responses. |
| Sandbox OS | Linux d’abord (bwrap + seccomp, Landlock en complément), x86_64 et ARM64, pour le parcours serveur de Brigade. Extension ensuite du Seatbelt macOS existant. |
| Base de travail | Relire et fusionner `feature/macro-host-integration` dans `main` avant le lot 0, car elle modifie les mêmes fichiers. |

## Profil AgSDL du module

| Domaine | Cible du module `argentic` |
| --- | --- |
| `read`, `write` | `harness` pour `allow`, `sandbox` pour `ask` et `deny`. Le module déclare aussi `read.outside` en mode `sandbox`. |
| `network` | `sandbox` pour `allow`, `ask` et `deny`. Le proxy réseau peut demander une approbation lorsque le shell vise un hôte soumis à `ask`. |
| `shell` | `harness`, comparaison du texte complet de chaque sous-commande. |
| `mcp`, `delegate` | `harness`. |
| `model.effort` | `approximate`. Si le fournisseur refuse le réglage d’effort, la tentative de travail se poursuit sans ce paramètre. |
| Autres propriétés déclarées | `native`, y compris `resources` (canal d’information distinct des instructions), `planFirst.ask` (verrou appliqué comme une permission), `reviewer.model` et `delivery.steer` (preuve de réception liée au work). |

Les exigences hors module s’appliquent aussi (`spec/agents.md`, `spec/flow.md`) :

- outil `agsdl_submit_result` avec correction dans la même instance ;
- `completionCheck` confiné aux permissions de l’agent ;
- `answerTimeoutMs` et horloge machine suspendue pendant les demandes à une
  personne ;
- instances `new`, partagées et nommées ;
- arrêt avec preuve de terminaison physique ;
- `harnessSettings` validés contre le schéma du module.

En lot 2, les outils de fichiers peuvent écrire dans le workspace. Une
demande `reviewer.person` passe par le port d’approbation du moteur. Le shell
Linux utilise Bubblewrap et seccomp. Il reçoit une copie des fichiers lisibles
et republie les modifications de fichiers ordinaires seulement quand tous les
plafonds autorisent la lecture et l’écriture de tout le workspace. Le réseau
exige une autorisation générale. Les chemins en dehors du workspace et les
répertoires supplémentaires ne sont pas exposés au shell. Les contraintes
d’installation et de publication sont détaillées dans le
[guide du harnais](../crates/argentic-harness/README.md#linux-shell).

## Architecture

```
crates/argentic-harness        boucle pure ; le moteur reste propriétaire de la durabilité
  loop/        tour composé d’étapes, hooks prepareRequest, before/afterToolCall, finishTurn
               steer consommé aux points sûrs (après les outils, en fin de tour)
               annulation hiérarchique, délai de grâce puis arrêt forcé
               compaction proactive et sensible au cache, contenus protégés (plan, skills)
  providers/   trait Provider, trois protocoles, variantes provider/model#variant
               relance sans effort à l’échelle de la tentative, prewarm, Retry-After
  tools/       exposition Direct/Deferred avec tool_search, portail RwLock pour le parallélisme
               file de mutation par fichier, troncature 2000 lignes / 50 KiB avec débordement en fichier
               read, list, glob, grep, ast_grep, edit, apply_patch, shell (PTY plafonné),
               web_fetch, plan, agsdl_submit_result, skill, subagent
  policy/      évaluation AgSDL : règles qui correspondent → plus restrictif, puis preset, puis zone
               globs * et ** compilés une fois, shell découpé par tree-sitter
               chaîne de relecteurs personne → modèle → hôte, issues fermées, refus par défaut
               mode sans humain : un ask non couvert devient deny
               délégation : intersection des politiques parent et enfant
  sandbox/     Linux bwrap + seccomp (+ Landlock), macOS Seatbelt
               proxy réseau local par hôte, chaque redirection réévaluée, durcissement du processus
  mcp/         stdio et Streamable HTTP, pool partagé
  protocol/    même processeur en mémoire et sur stdio, trames versionnées, champs inconnus tolérés
crates/argentic-runtime/src/agsdl_host_argentic030/
               adaptateur AgentExecutor030 : événements, steer, approbations, arrêt physique
crates/argentic-core/assets/harnesses/argentic/module.json
               module 1.0.0, à proposer ensuite dans AgSDL
```

Les `harnessSettings` exposent les réglages que la spec laisse au harnais :
limites de tours et de tokens, compaction, exposition des outils, surcouches du
prompt système, hooks et plafonds de ressources. La configuration est en
couches avec provenance. Une couche de projet ne peut qu’abaisser les plafonds.

La compaction d’Argentic utilise le champ optionnel `contextWindowTokens`
du fournisseur dans la configuration du Host. Cette taille s’applique à tous
les modèles de cette entrée. Le réglage `harnessSettings.compaction` accepte
`enabled`, `threshold` et `keepRecentTurns`. Le seuil par défaut est de 80 %
de la fenêtre estimée, sous réserve de la place nécessaire à la réponse.
Argentic garde les quatre derniers tours par défaut.
L’historique retenu a un plafond de 32 Mio, même si la compaction est désactivée.
L’événement `argentic.contextCompaction` contient les compteurs de réduction.

Lorsqu’un tour se termine sans la soumission requise, Argentic demande au modèle
d’appeler `agsdl_submit_result` et impose cet outil sur la requête de correction.
`harnessSettings.forceSubmissionOnCorrection` vaut `true` par défaut.
Si le fournisseur refuse le choix nommé, Argentic relance avec `required`
(`any` pour Anthropic Messages) et le seul outil de soumission. S’il refuse
aussi cette forme, Argentic désactive le choix forcé pour le reste de la
tentative et rétablit tous les outils. Les corrections suivantes de la même
tentative reprennent au niveau de repli atteint.

Une requête qui transmet un message de pilotage laisse le modèle choisir
parmi tous les outils. Seule une nouvelle erreur `SUBMISSION_REQUIRED` peut
réactiver le choix forcé. Anthropic Messages utilise la sélection automatique
des outils en mode de réflexion étendue manuelle.

Les instances partagées et nommées conservent leur conversation en mémoire
pendant le run, y compris après l’arrêt d’un travail. Un redémarrage du Host
marque les runs actifs comme interrompus. Le journal permet de rejouer les
transitions du moteur. Les transcriptions des outils restent en mémoire.

## Reprises depuis Macro

| Élément | Source dans Macro | Traitement |
| --- | --- | --- |
| Moteur de patch | `src-tauri/src/core/workspace_execution/mod.rs:371-690` | Porter, remplacer `CommandError`. |
| Pagination et plafonds de sortie | `src-tauri/src/core/workspace_execution/tool_output.rs` | Porter tel quel. |
| Recherche AST | `src-tauri/src/core/workspace_execution/ast_search.rs` | Porter, brancher l’accès fichier confiné d’Argentic. |
| Accès fichier confiné | `src-tauri/src/fs/confined.rs` | Rapprocher de `content_io/admitted.rs`. |
| Flux SSE Chat Completions | `src-tauri/src/ai/openai_compatible.rs` | Extraire l’analyseur, retirer Tauri, la base et les secrets Macro. |
| Catalogue de raisonnement | `src-tauri/src/ai/reasoning_catalog.rs`, `provider_capabilities.rs` | Porter avec le JSON. |
| Relance sans effort | `src/services/ai/chatCompletionsAdapter.ts:175-201` | Réécrire en Rust. |
| Outil de soumission typé, approbations consommées une fois | `toolDefinitions.ts`, `db/agsdl_execution/approvals.rs` | Reprendre le modèle, pas le code. |
| Politique d’outils, montages virtuels, mémoire en liste brute | Divers | Abandonner. |

## Inspirations externes

Les dépôts de `~/github/harness` sont des sources tierces. Seules les licences
Apache-2.0 et MIT permettent une copie. `codex` porte un `NOTICE`, et son
bubblewrap vendu est sous LGPL : utiliser le `bwrap` du système.

| Idée | Source |
| --- | --- |
| Orchestrateur unique décision → sandbox → nouvel essai après approbation | `codex/codex-rs/core/src/tools/orchestrator.rs` |
| Règles de préfixe avec exemples validés au chargement | `codex/codex-rs/execpolicy` |
| bwrap + seccomp, proxy réseau par hôte, durcissement | `codex/codex-rs/linux-sandbox`, `network-proxy`, `process-hardening` |
| Algèbre de politique override/restrict/fallback, effets atteignables pour masquer les outils | `opencode/packages/core/src/permission/policy.ts` |
| Compaction précalculée et réductions successives | `opencode/packages/core/src/session/compaction.ts` |
| Noyau de boucle à hooks et files steer/follow-up | `pi/packages/agent/src/agent-loop.ts` |
| Steer aux points sûrs, bail d’exécution par tour | `jcode/docs/SOFT_INTERRUPT.md`, `SESSION_RUNTIME_OWNERSHIP.md` |
| Pools partagés, `Arc` avec copie à l’écriture, hibernation | `jcode/docs/MEMORY_BUDGET.md`, `SESSION_HIBERNATION.md` |
| Isolation copie-sur-écriture des sous-agents | `oh-my-pi/crates/iso` |
| Relecteur modèle APPROVE/DENY/ESCALATE, planchers avant contournement | `hermes-agent/tools/approval_smart.py`, `approval_floors.py` |

À éviter : redirections réseau vérifiées seulement contre deny (opencode
`webfetch`), politique d’un sous-agent non intersectée avec celle du parent
(opencode, deepagents), joker `*` qui traverse `/`, lecture seule tenue par le
seul prompt, détection de commandes dangereuses par regex, absence de sandbox
OS (jcode, pi).

## Codage en dur sur Codex à lever (lot 0)

1. Un seul exécutant : `agsdl_host_runtime030.rs:557`, construit par `api.rs:340-357`. Router par `agent.harness`.
2. `standalone.rs:388-441` : refus des autres harnais, opt-in, observation et registre limités à Codex.
3. `agsdl_host_qualification030.rs:76-99` : « Only the Codex harness » et refus associés.
4. `agsdl_host_work_admission030.rs` : `from_codex` seul constructeur de production, `validate_codex_registry030`, `codex_permissions030`, `codex_read_only030`, `validate_codex_document_at030`, branches `== "codex"`.
5. `capture.rs:64-170` : capacités et preuve `native-codex` en dur.
6. `argentic-core/src/compile/plan.rs:336-345` : `productRuntimeAvailable` exige Codex.
7. `agsdl_host_completion030.rs:33` : réutilise `codex_read_only030`.
8. Diagnostics de runtime et d’approbation qui dépendent de `agsdl_host_codex::diagnostics`.
9. Approbations, contenus et skills rangés sous `agsdl_host_codex030`, à remonter en modules communs.
10. Surface publique de `api.rs:223-284` et `host.rs:69-98` limitée à Codex.
11. `agsdl_host_harness030::observe` limité à opencode et pi.
12. Sémantique steer 0.4.0 testée dans l’adaptateur Codex, à remonter dans `SteerDelivery030`.

`permissions/enforcement.rs` contient déjà un squelette de qualification par
action et effet, inutilisé. Il servira à prouver le profil du module.

## Lots

| N° | Contenu | Résultat attendu |
| --- | --- | --- |
| 0 | Fusionner `feature/macro-host-integration`, router les exécutants par harnais, lever les points ci-dessus, créer le module `argentic` 1.0.0. | Codex inchangé, tests verts, un second exécutant enregistrable. |
| 1 | Crate, boucle, fournisseurs (Chat Completions d’abord), outils de lecture, `agsdl_submit_result`, `instance.new`, preset `read-only`. | Scénarios de conformité rejoués avec un fournisseur scripté. |
| 2 | Moteur de politique, outils d’écriture, shell sous sandbox Linux, `reviewer.person`. | Matrice action × effet prouvée par `enforcement.rs` sur Linux. |
| 3 | Steer et livraison, instances partagées et nommées, compaction, effort, Anthropic Messages et Responses. | Scénarios steer et arrêt de la 0.4.0 verts. |
| 4 | MCP, skills, `resources`, `directories`, proxy réseau, Seatbelt macOS. | Profil complet sur Linux et macOS. |
| 5 | `plan`, `planFirst` auto et ask, `reviewer.model`, délégation. | Propriétés du lot `native`, délégation appliquée par le harnais. |
| 6 | Performance, relais natif, préparation du shell, pools partagés et hibernation. | Implémenté. La CI Linux relève la latence et la taille de l’historique au repos. |


### Lot 4a

Les ressources de l’agent et celles des messages sont transmises dans un message
de référence distinct de la mission. Le message indique leur origine et leur
absence d’autorité. Le résolveur commun ouvre les URI locales sous les permissions
de lecture du destinataire. L’adaptateur accepte le texte et le JSON.

L’outil `skill` présente les noms et descriptions des skills sélectionnés. Il
active le corps d’un skill sur demande et donne accès à ses fichiers texte ou
JSON, sous les mêmes permissions de lecture. La compaction conserve l’échange
d’activation.

Argentic et Codex partagent la découverte et l’exécution des outils MCP. Argentic
associe chaque nom exposé au modèle au couple `serveur/outil` du document.
Une règle `ask` suspend l’appel jusqu’à la décision d’une personne. Sans
relecteur humain, l’appel est refusé. Le transport stdio confiné reste qualifié
sur macOS ARM64. Streamable HTTP utilise l’autorisation du Host et son délai
d’appel. Lors d’un Stop, un appel distant déjà transmis et dont la fin ne peut
être attestée produit `MCP_TERMINATION_UNCERTAIN`. La mise en pause opérateur
est indisponible pour les runs qui sélectionnent des serveurs MCP.

Le workspace conserve l’indice 0. Les autres répertoires autorisés sont
accessibles par indice dans les outils fichiers et montés sous
`/directories/<indice>` dans le shell Linux. Le shell utilise des copies
filtrées. Il publie leurs modifications lorsque chaque politique effective
accorde la lecture et l’écriture sans restriction sur le répertoire concerné.
L’admission refuse les racines qui se recouvrent pour le shell ou recouvrent
les données privées du Host.

### Lot 4b

Le shell Linux conserve le namespace réseau privé et seccomp. Un relais TCP
sur le loopback du namespace transmet les requêtes à un socket Unix du Host.
Sur macOS ARM64, Seatbelt interdit le réseau direct et autorise uniquement
le port TCP local du proxy. Les clients qui prennent en charge les variables
de proxy peuvent utiliser HTTP et HTTPS. Le mode `blanket`, par défaut,
exige une autorisation réseau globale dans chaque couche de politique. Il
transmet les octets du tunnel HTTPS. Le mode `intercept` applique les règles
par hôte, déchiffre HTTPS avec une autorité de certification propre à
l’invocation et vérifie la correspondance entre l’hôte du CONNECT, le SNI
et l’autorité des requêtes HTTP.

Le proxy applique chaque politique effective à l’hôte demandé. Un refus
renvoie une erreur HTTP 403. Une demande `ask` passe par le port partagé
d’approbation humaine en mode `intercept` et est refusée en l’absence de
relecteur. Le proxy demande une décision par hôte et par invocation, suspend
les délais machine pendant l’attente et retire la demande si le client se
déconnecte. Chaque requête issue d’une redirection repasse par les contrôles.
Le proxy résout lui-même le nom, contrôle les adresses obtenues et se
connecte à l’adresse vérifiée.
Une adresse privée, de loopback ou de lien local exige une règle IP exacte.
Les budgets bornent les connexions, les octets échangés et leur durée.

Le shell Seatbelt réutilise la préparation et la publication des copies
filtrées. Son répertoire courant est la copie du workspace. Les autres
racines sont exposées sous `ARGENTIC_DIRECTORY_<indice>`. Le profil limite
l’exécution à Bash et à une liste d’utilitaires système, et le lanceur vérifie
que les processus de la session dédiée sont terminés avant de publier.
Windows refuse le shell à la qualification.

### Lot 5

`plan: true` expose un outil `plan` avec des étapes et leurs statuts. Chaque
mise à jour est un événement `planPresented` dans le journal du travail.
La boucle réinjecte le plan courant dans les requêtes au modèle, hors de
l’historique que la compaction peut résumer.
[AgSDL 0.4.0, plan](https://github.com/Andrologic/AgSDL/blob/v0.4.0/spec/agents.md#plan)

Le verrou de `planFirst` est recréé pour chaque travail, y compris sur une
instance partagée ou nommée. Tant qu’il est actif, la boucle refuse les mutations,
le shell, les appels MCP et la délégation. Elle diffère aussi la découverte MCP,
car l’initialisation d’un serveur peut avoir des effets externes. En mode
`auto`, la présentation lève le verrou. En mode `ask`, le Host traite la
réponse humaine avec le courtier d’approbation et le verrou natif du moteur.
Un refus commenté conserve le verrou et restitue le commentaire. Un refus
sans commentaire, une échéance atteinte ou l’absence de relecteur humain
termine le travail avec `PLAN_REJECTED`.
[AgSDL 0.4.0, plan first](https://github.com/Andrologic/AgSDL/blob/v0.4.0/spec/agents.md#plan-first)

Le Host sélectionne le relecteur dans son catalogue de fournisseurs avec
`reviewerModel`. Le modèle reçoit l’action exacte, son contexte de politique
et des extraits bornés de la conversation. Sa réponse JSON est limitée à
`APPROVE`, `DENY` ou `ESCALATE`. Une erreur, un délai dépassé ou une réponse
invalide produit un refus. Le Host exige une personne pour les demandes
`ask` du shell, du réseau via le proxy et de MCP. Il vérifie la politique
courante avant d’appliquer une acceptation. Le journal conserve le verdict
et sa provenance. Les arguments bruts transmis au relecteur ne figurent pas
dans `reviewerDecision`.
[AgSDL 0.4.0, reviewer](https://github.com/Andrologic/AgSDL/blob/v0.4.0/spec/agents.md#reviewer)

`delegate` applique les plafonds effectifs du parent à l’action de
délégation. Le sous-agent reçoit une conversation neuve et un registre de
lecture seule intersecté avec les plafonds du parent. Il peut lire les
racines admises sous les règles de lecture héritées. Les lectures `ask`
sont refusées dans le sous-agent, qui n’a accès ni au shell ni au réseau.
Une délégation est synchrone, avec un seul niveau et un maximum de huit
tours par enfant. Les tours et les
jetons facturés à l’enfant sont décomptés du budget du parent. Le parent
reçoit la réponse finale de l’enfant ou une erreur d’outil. Stop annule le
flux du fournisseur et attend les lecteurs de fichiers de l’enfant avant de
confirmer la fin physique. La restriction à la lecture seule évite toute
publication de changements ou copie du workspace.
[AgSDL 0.4.0, règles des hôtes](https://github.com/Andrologic/AgSDL/blob/v0.4.0/spec/agents.md#rules-for-hosts) et [arrêt d’un travail](https://github.com/Andrologic/AgSDL/blob/v0.4.0/spec/flow.md)

Le module 1.2.0 du Host déclare `plan`, `planFirst.auto`, `planFirst.ask` et
`reviewer.model` comme `native`. Les trois effets de `delegate` sont
appliqués par le harnais. Le lot reprend le principe des trois verdicts de
Hermes Agent et l’objectif d’isolation d’oh-my-pi en Rust, sans reprendre
leur code.
[AgSDL 0.4.0, modules de harnais](https://github.com/Andrologic/AgSDL/blob/v0.4.0/spec/harnesses.md)


### Lot 6

Bubblewrap installe le relais Rust embarqué sur sa racine en lecture seule.
Le relais écoute sur le loopback privé et relaie les connexions au socket
Unix du proxy hôte. La fin du namespace ferme les connexions restantes.

Chaque commande reçoit des inodes privés. Linux utilise `FICLONE` quand le
système de fichiers permet de partager les blocs avec la source. Le repli
copie les données avec jusqu’à quatre travailleurs et une file bornée.
Le contrôle de publication conserve les empreintes des fichiers au lieu
de leur contenu initial. Avant chaque écriture, il compare aussi le `ctime`
et le SHA-256 du fichier hôte. Une écriture d’un autre processus entre ce
contrôle et le renommage final reste possible, faute de verrou commun. Chaque racine de shell accepte 50 000 entrées et
256 Mio, avec 8 Mio par fichier source ou publié. Le contrôle périodique
du shell écrivable applique les mêmes plafonds d’entrées et de taille totale.

Les fournisseurs partagent un client HTTP dont les connexions sont séparées
par origine. Le client négocie HTTP/2 avec les serveurs compatibles. Chaque
travail garde ses propres identifiants et ses décisions de repli en cas de
refus d’un réglage par le fournisseur. Un run partage
ses politiques compilées et ses définitions d’outils par contrat. Le
catalogue de raisonnement est chargé une fois par processus. Dans les
requêtes, le plan et le steer peuvent être ajoutés temporairement sans
dupliquer l’historique.

Entre deux travaux, le Host compresse l’historique d’une instance continue
à partir de 64 Kio si cela économise au moins un quart de sa taille estimée.
Il restaure le contenu et les données de reprise propres au fournisseur au
travail suivant.

Mesures locales en profil debug sur Linux ARM64, avec la copie de repli sur
ext4. Les temps de préparation sont les médianes de trois passages et
excluent la génération
des fichiers, le lancement de Bash et la publication. Le tour scripté
est la médiane de 256 tours sans appel réseau. La mesure mémoire suit les
octets de la conversation et ceux que sa représentation conserve en mémoire.

| Mesure | Avant | Après |
| --- | ---: | ---: |
| Préparation, 20 000 fichiers et 200 Mo | Refus à 64 Mio | 263 ms |
| Préparation, 20 000 fichiers et 60 Mo | 543 ms | 250 ms |
| Préparation du dépôt Argentic | 22 ms | 26 ms |
| Tour scripté | 96 µs | 80 µs |
| Historique au repos, fixture compressible | 123 108 octets d’historique | 1 083 octets retenus |

La CI Linux affiche les mesures avec `shell_staging`,
`scripted_turn_overhead` et `idle_history_round_trips_provider_replay`.
Elle signale les fortes régressions au-delà de 8 s pour préparer un arbre,
5 ms par tour scripté ou un quart des octets de la conversation pour
l’historique au repos sur la fixture compressible.

La recette de lecture de quatre fichiers suivie d’une soumission a réussi
6 fois sur 6 avec LM Studio et `qwen/qwen3-4b-2507`.
