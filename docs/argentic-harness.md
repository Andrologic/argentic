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

## Profil AgSDL visé

| Domaine | Cible du module `argentic` |
| --- | --- |
| `read`, `write`, `network`, `outside` | `sandbox` pour allow, ask et deny. L’OS interdit l’accès direct. Le proxy réseau peut demander une approbation au moment où le shell vise un hôte soumis à `ask`. |
| `shell` | `harness`, comparaison du texte complet de chaque sous-commande. |
| `mcp`, `delegate` | `harness`. |
| Toutes les propriétés | `native`, y compris `resources` (canal d’information distinct des instructions), `planFirst.ask` (verrou appliqué comme une permission), `reviewer.model` et `delivery.steer` (preuve de réception liée au work). |

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
| 5 | `plan`, `planFirst` auto et ask, `reviewer.model`, délégation. | Toutes les propriétés `native`. |
| 6 | Performance : pools partagés, données immuables partagées, hibernation. | Mesures mémoire et latence par session, suivies en CI. |


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
de proxy peuvent utiliser HTTP et HTTPS. Pour HTTPS, le proxy vérifie la
destination de CONNECT, puis transmet les octets du tunnel.

Le proxy applique chaque politique effective à l’hôte demandé. Un refus
renvoie une erreur HTTP 403. Une demande `ask` passe par le port partagé
d’approbation humaine et est refusée en l’absence de relecteur. Chaque requête
issue d’une redirection repasse par les contrôles. Le proxy résout lui-même
le nom, contrôle les adresses obtenues et se connecte à l’adresse vérifiée.
Une adresse privée, de loopback ou de lien local exige une règle IP exacte.
Les budgets bornent les connexions, les octets échangés et leur durée.

Le shell Seatbelt réutilise la préparation et la publication des copies
filtrées. Son répertoire courant est la copie du workspace. Les autres
racines sont exposées sous `ARGENTIC_DIRECTORY_<indice>`. Le profil limite
l’exécution à Bash et à une liste d’utilitaires système, et le lanceur vérifie
que les processus de la session dédiée sont terminés avant de publier.
Windows refuse le shell à la qualification.
