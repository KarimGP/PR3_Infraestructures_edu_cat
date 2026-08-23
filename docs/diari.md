# Diari d'aprenentatge — PR3

## Com funciona això

Cinc minuts al final de cada dia. **Sense mirar el xat ni la documentació.**
Si has de consultar-ho per escriure-ho, és que encara no ho tens, i això
és precisament el que volem detectar.

Tres preguntes per dia:

1. **Què he fet avui** — en dues o tres frases, com si ho expliquessis a
   un company que no hi era.
2. **Per què** — quina decisió hi havia darrere. Aquesta és la que compta
   a l'entrevista; el "què" el pot llegir qualsevol al repositori.
3. **Què ha fallat i com ho he diagnosticat** — el procés de depuració,
   no només la solució final.

Marca amb `[?]` tot allò que no puguis explicar. **Els interrogants són
el més valuós del diari**, no un senyal de fracàs: són el temari exacte
que has de tancar abans del 4 de setembre. Els repassarem als dies 7, 14
i 19, que ja són de marge.

Escriu malament, amb frases a mitges i faltes. Això no és documentació
per publicar; és per a tu. La documentació neta és el dia 18.

---

## Dia 1 — Dilluns 17 · Infraestructura base

*Per omplir demà. Recordatoris del que va passar, perquè no se't perdi
el fil — però l'explicació l'has d'escriure tu.*

### Què he fet

*Paraules clau: WSL2, Docker Desktop, docker compose up, esquema SQL
automàtic, imatge Flink personalitzada, prova de foc datagen → JDBC.*

### Per què

Intenta respondre aquestes. Si alguna et deixa en blanc, `[?]` i endavant:

- Per què Docker, i no instal·lar PostgreSQL directament a Windows?
- Per què els fitxers SQL es col·loquen a `/docker-entrypoint-initdb.d`
  en comptes d'executar-los a mà? Què implica que només corrin un cop?
- Per què una imatge pròpia de Flink en comptes de l'oficial?
- Per què vam fixar les versions dels JARs abans de començar?
- Per què tres esquemes (`ops`, `bronze`, `meta`) i no tot junt?
- Per què la prova de foc feia servir `datagen` i no Kafka?

### Què ha fallat

Tres incidents. Descriu el símptoma, la hipòtesi i com es va confirmar:

1. **WSL2** — `Error catastrófico` a la instal·lació.
2. **Zookeeper `unhealthy`** — i el seu efecte en cadena sobre Flink.
3. **Rutes a Git Bash** — `NoSuchFileException` a PyFlink, dos intents
   fallits abans d'encertar-la.

### Interrogants oberts

- [?]

---

## Dia 2 — Dimarts 18 · Dades de referència i sintètiques

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 3 — Dimecres 19 · Kafka i productor

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 4 — Dijous 20 · Flink amb Kafka

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 5 — Divendres 21 · Flink → Bronze

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 6 — Dissabte 22 · Agregació amb finestres

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## 🛟 Dia 7 — Diumenge 23 · PRIMERA PAUSA DE CONSOLIDACIÓ

No és un dia de diari normal. Repassa tots els `[?]` de la setmana i
prova de respondre'ls. Els que segueixin oberts, els treballem.

Preguntes de control de la setmana 1 — respon-les en veu alta, com si
fossis a l'entrevista:

- Explica l'arquitectura del projecte en dos minuts, sense mirar res.
- Per què Kafka necessita dos listeners diferents?
- Què passa si el TaskManager de Flink no s'ha registrat al JobManager?
- Què és un healthcheck i per què un de mal configurat bloqueja serveis
  que no tenen res a veure amb ell?
- Si haguessis de muntar aquest stack de zero en una màquina nova, quin
  seria el primer pas i per què?

---

## Dia 8 — Dilluns 24 · dbt: arrencada i staging

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 9 — Dimarts 25 · dbt: intermediate i marts

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 10 — Dimecres 26 · dbt: tests

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 11 — Dijous 27 · dbt: documentació i llinatge

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 12 — Divendres 28 · Dataset d'ML

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 13 — Dissabte 29 · Prophet vs XGBoost

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## 🛟 Dia 14 — Diumenge 30 · SEGONA PAUSA DE CONSOLIDACIÓ

Preguntes de control de la setmana 2:

- Què és exactament una capa de staging i per què no hi pot haver
  lògica de negoci?
- Per què materialitzar unes taules com a `view` i altres com a
  `incremental`?
- Diferència entre un test genèric i un de singular a dbt.
- Per què el split del dataset ha de ser temporal i no aleatori?
- Si el teu model no bat la baseline, què fas?

---

## Dia 15 — Dilluns 31 · Power BI: model i pàgines 1–2

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 16 — Dimarts 1 · Power BI: pàgines 3–5

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 17 — Dimecres 2 · DCAT i dades obertes

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 18 — Dijous 3 · Documentació

### Què he fet

### Per què

### Què ha fallat

### Interrogants oberts

---

## Dia 19 — Divendres 4 · TERCERA PAUSA · Preparació d'entrevista

Repàs final de tots els `[?]` que quedin oberts.

Prepara aquestes tres respostes per escrit i assaja-les en veu alta:

**1. El projecte en dos minuts.** Problema, arquitectura, decisions
clau, resultat. Sense entrar en detall tècnic tret que et pregunten.

**2. La decisió de la qual estàs més orgullós.** Amb l'alternativa que
vas descartar i per què.

**3. El problema més difícil que vas resoldre.** Símptoma, hipòtesis,
com vas anar descartant, solució. El procés val més que el resultat.

---

### Una nota sobre honestedat

Quan et preguntin per una eina, la resposta forta no és fingir fluïdesa.
És:

> *"Vaig muntar aquest stack per a un projecte de portfolio. Aquestes
> són les decisions que vaig prendre i per què, i aquests els problemes
> que em vaig trobar i com els vaig diagnosticar. No he operat Flink en
> producció."*

Això és cert, és verificable, i aguanta la segona pregunta. La fluïdesa
fingida no.
