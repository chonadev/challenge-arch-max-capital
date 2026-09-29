# SPEC.md — Order Processing Service

Especificación funcional y de diseño para el challenge técnico de Max Capital.
Este documento es la fuente de verdad de la que se deriva la implementación
y los tests. Cada decisión está justificada contra el enunciado original
(`challenge-arch-max-capital.pdf`) o marcada explícitamente como supuesto propio.

---

## 1. Dominio

### 1.1 Entidades del dominio

**Orden** (`numericOrderId`): entidad de negocio con estado mutable. Recibe
múltiples ExecutionReports (ER) a lo largo de su vida.

**ExecutionReport (ER)**: evento inmutable, snapshot del estado de una
orden en un instante. Múltiples ER por orden es el caso normal, no una
anomalía.

### 1.2 Máquina de estados de una orden

```
NEW ──► PARTIALLY_FILLED ──► FILLED   (terminal)
  │            │
  └────────────┴──────────► CANCELLED (terminal)
```

**Invariante I1**: toda orden arranca con exactamente un ER de status `NEW`.
Un ER no-`NEW` para una orden inexistente es una violación de secuencia
(ver §4.3).

**Invariante I2**: ningún ER puede aplicarse sobre una orden en estado
terminal (`FILLED` o `CANCELLED`). Fuente: PDF, sección "Qué tenés que
construir", punto 6.

**Invariante I3**: los campos de cantidad (`leavesNominalAmount`,
`accumulativeNominalAmount`) son snapshots acumulados, no deltas — cada
ER trae el valor total vigente a ese momento, no un incremento.

### 1.3 Identidad de los datos

| Concepto | Identificado por | Uso |
|---|---|---|
| Una orden | `numericOrderId` | PK de la tabla `orders` |
| Un ER individual (para dedup) | `(secondaryTradeId, operationNumber)` | UNIQUE constraint en `execution_ledger` |
| Un settlement (para dedup) | `numericOrderId` | UNIQUE constraint en `outbox` |

**Decisión D1 — clave de dedup del ER**: el PDF no dice explícitamente
"usá el campo X como clave de dedup", pero marca literalmente
`secondaryTradeId` y `operationNumber` con el comentario `// identidad
de la ejecución (para dedup)`. Se descarta `fixId` como clave de dedup
porque el PDF no lo marca como tal, aunque aparezca primero en la
estructura del mensaje.

---

## 2. Garantías a sostener (contratos del sistema)

### G1 — Secuencia por orden, paralelismo entre órdenes
Los ER de una misma orden se aplican en el orden en que fueron emitidos.
Entre órdenes distintas, el orden relativo no importa y pueden procesarse
en paralelo, incluso con 2 instancias del servicio corriendo en simultáneo.

**Mecanismo**: particionado de Kafka por `numericOrderId` como key del
mensaje. Kafka garantiza orden dentro de una partición; el mismo key
siempre resuelve a la misma partición (hash determinístico). Ambas
instancias comparten `consumer group`, así que cada partición es
consumida por exactamente un miembro del grupo a la vez.

**Lo que se resigna**: no hay garantía de orden entre órdenes que
comparten partición si el productor las intercala — no hace falta,
el PDF exige orden solo intra-orden.

### G2 — Idempotencia
Un ER duplicado o reentregado no corrompe el estado. La identidad de un
duplicado es el ER individual (ver Decisión D1), no la orden.

**Mecanismo**: `INSERT ... ON CONFLICT (secondary_trade_id,
operation_number) DO NOTHING` sobre `execution_ledger`. Es atómico: no
hay ventana entre "chequear si existe" y "insertar" donde otro proceso
pueda colarse.

**Complementario, no sustituto**: `enable.idempotence=true` en el
producer de Kafka. Resuelve un problema distinto (duplicado por reintento
de red del producer), no el de negocio. Ver §5 para la distinción completa.

### G3 — Estado de la orden refleja fielmente su secuencia
El estado no se sobreescribe con el último ER — se computa a partir del
estado ya persistido más el ER entrante. La transición de `status` se
valida contra el status ya guardado; `executionsCount` se incrementa
sobre el valor anterior, nunca se recalcula desde cero.

**Mecanismo**: `SELECT ... FOR UPDATE` (lock pesimista) sobre la fila de
la orden antes de leer/decidir/escribir, dentro de una única transacción
que también hace el insert idempotente del ledger.

**Por qué el lock, si el particionado ya da orden**: el particionado es
la primera línea de defensa (evita la carrera en el caso normal). El lock
es la segunda línea, para la ventana breve de un rebalanceo de consumer
group donde en teoría podría haber superposición. Ver Decisión D2.

### G4 — Recuperación ante fallas sin pérdida ni duplicación
Si una instancia cae a mitad de procesamiento, el ER no se pierde ni se
aplica dos veces al reiniciar.

**Mecanismo**: `ack-mode: record` en el consumer de Kafka (configurado
en `KafkaConsumerConfig`, no en el yml). El offset avanza solo después de
que el handler del listener retorna sin excepción — y `OrderProcessingService.apply`
corre dentro de una transacción que commitea antes de volver. Si el
proceso muere antes del commit, el offset no avanzó y Kafka reentrega el
mismo mensaje al reiniciar; G2 (idempotencia) absorbe esa reentrega sin
duplicar.

**Por qué `record` y no `manual`**: con `@RetryableTopic`, el manejo de
offsets y de la DLQ lo administra Spring Kafka, y `AckMode.RECORD` es el
que espera esa infraestructura. Un `MANUAL` explícito entraría en
conflicto con el reintento automático: para poder reintentar el mismo
mensaje en un topic de retry, el offset del original no debe haberse
confirmado todavía.

### G5 — Manejo de errores sin pérdida silenciosa ni bloqueo indefinido
Un ER que falla no se descarta sin dejar rastro, ni bloquea
indefinidamente el flujo de su partición (y por extensión, de las demás
órdenes de esa partición).

**Taxonomía de errores**:
- **Permanente** (no reintentable): JSON malformado, texto plano no JSON, campos obligatorios
  faltantes, o transición de estado inválida (`PermanentProcessingException`, `InvalidStateTransitionException`). 
  Se excluyen de los reintentos (`exclude`), registrando un `log.error` inmediato y derivándolos directamente a `er.raw.dlq` sin bloquear el flujo.
- **Transitorio** (reintentable): fallo de conexión a DB, timeout de red.
  Se gestionan automáticamente mediante `@RetryableTopic` (3 intentos con backoff exponencial) y, si se agotan, se envían a la DLQ.

**Deserialización Segura**:
- Uso de `ErrorHandlingDeserializer` en `KafkaConsumerConfig` para prevenir bucles infinitos de reintentos ante poison pills o strings malformados, entregando un valor `null` que la validación rutea a DLQ.

### G6 — Settlement exactamente una vez
Cuando una orden llega a `FILLED`, se publica un evento de settlement
downstream exactamente una vez por orden, sin duplicados ni pérdidas,
incluso ante reentregas de ER o con las dos instancias corriendo.
`CANCELLED` no emite settlement (regla explícita del PDF).

> **Precisión sobre el título**: la garantía real del sistema es
> *at-least-once* con deduplicación a cargo del downstream, no
> exactly-once estricto. Ninguna combinación de outbox + lock sin
> transacción distribuida entre la DB y el broker puede dar
> exactly-once: siempre queda la ventana entre el ack del broker y el
> commit del marcador local. Ver "Límite de la garantía" más abajo.

**Por qué no alcanza con publicar directo a Kafka al detectar FILLED**:
el "dual write" (DB + broker sin transacción compartida) puede perder el
evento si el proceso muere entre el commit de la orden y el `send()` a
Kafka. Ver Decisión D4 para el análisis completo con línea de tiempo.

**Mecanismo — Outbox Pattern**:
1. Al aplicar el ER que lleva la orden a `FILLED`, se inserta una fila en
   `outbox` en la **misma transacción** que el update de la orden.
   Atomicidad DB-vs-DB, trivial de lograr.
2. Un proceso poller separado (`@Scheduled`, cada 500ms) **reclama** un
   batch de filas pendientes y las publica a Kafka.
3. Solo tras confirmar el ack del broker (`.get()` bloqueante sobre el
   `Future` del `send()`), se marca `published=true`.

**Exclusión mutua entre los dos pollers — claim, no solo `SKIP LOCKED`**

El `SELECT ... FOR UPDATE SKIP LOCKED` **no alcanza** para garantizar
que un evento se publique una sola vez. El lock se libera cuando la
transacción de lectura commitea, pero el `send()` a Kafka y el
`markAsPublished()` pasan *después*, en transacciones separadas:

```
t1  poller A: SELECT ... FOR UPDATE SKIP LOCKED -> fila, tx commitea, lock liberado
t2  poller A: kafka.send().get()                  -> ack del broker
t3  poller A: markAsPublished(id)                 -> published=true

    ... en paralelo, si el SELECT de B cae entre t1 y t3:
t1' poller B: SELECT ... FOR UPDATE SKIP LOCKED -> MISMA fila (published=false)
t2' poller B: kafka.send().get()                  -> ack  => PUBLICADO DOS VECES
```

`SKIP LOCKED` solo coordina a los dos `SELECT` **si se solapan en el
tiempo**. Con un poller cada 500ms por instancia, es normal que no se
solapen, y entonces la garantía se pierde. Esto se observaba como un
porcentaje de duplicados, no como un fallo consistente.

**Mecanismo que sí cierra la ventana — `locked_at` (claim)**:

1. `claimPendingBatch` corre **una sola transacción** que hace el
   `SELECT ... FOR UPDATE SKIP LOCKED` filtrando por
   `locked_at IS NULL OR locked_at < now() - stale`, y acto seguido
   escribe `locked_at = now()` en esas mismas filas.
2. Al commit, el claim queda **durable**: cuando los locks se liberan, la
   fila ya está marcada como tomada y el otro poller la excluye aunque su
   `SELECT` arranque después.
3. Un claim más viejo que `app.outbox.claim-stale-secs` (60s) se
   considera de un poller caído y la fila vuelve a ser reclamable.
   Recuperación ante falla sin intervención manual.
4. Si el publish falla, se libera el claim (`locked_at = NULL`) para
   reintentar en el siguiente ciclo, sin esperar el vencimiento.

**Por qué el claim se escribe en la misma transacción que el SELECT, y no
después**: si el `UPDATE` fuera una segunda transacción, existiría otra
ventana entre el commit del `SELECT` y el commit del `UPDATE` —
exactamente la misma carrera, un paso más adentro.

**Límite de la garantía**: el claim cierra la exclusión mutua *entre
pollers vivos*, pero no la ventana del crash individual. Si el proceso
muere entre `t2` (ack del broker) y `t3` (`published=true`), el evento
se republica en el próximo ciclo. Por eso:

- `UNIQUE(numeric_order_id, event_type)` en `outbox` evita doble *fila*
  por la misma orden (defensa en profundidad, no sustituto del claim).
- El **consumidor downstream** (no implementado, según alcance del PDF)
  debe deduplicar por `numericOrderId`. Es la única capa que puede
  cerrar esa ventana.

Cubierto por `OutboxClaimTest` (spec §4.9).

---

## 3. Decisiones de infraestructura

### D2 — Kafka como broker, número de particiones
Elegido sobre RabbitMQ porque el particionado por key resuelve
nativamente G1 (orden intra-partición, paralelismo entre particiones)
sin tener que construir un mecanismo de locking distribuido propio.
RabbitMQ requeriría un consistent-hash exchange o colas por hash de
orden para lograr algo equivalente.

**6 particiones**: elegido para repartirse parejo entre 2 instancias (3
cada una) y dejar margen de escalado a una tercera/cuarta instancia sin
recrear el topic. No hay un cálculo más profundo detrás — es una
decisión de diseño razonable, no derivada de un requisito numérico del
PDF.

### D3 — Postgres como motor de persistencia
Elegido por dos capacidades que el diseño necesita de forma nativa:
1. Transacciones ACID con `SELECT ... FOR UPDATE` multi-tabla (lock +
   insert ledger + update orden + insert outbox, todo atómico).
2. `UNIQUE constraint` + `ON CONFLICT DO NOTHING` como mecanismo de
   idempotencia a nivel DB, no solo a nivel aplicación.

Un motor NoSQL permitiría lo segundo pero no lo primero sin esfuerzo
adicional significativo.

### D4 — Por qué el Outbox y no solo idempotencia simple
Análisis de la ventana de pérdida con idempotencia simple (insert de
"ya publicado" + `send()` como dos pasos separados, sin outbox):

```
t1: BEGIN TX
t2: UPDATE orders SET status='FILLED'
t3: INSERT INTO settlement_published (numeric_order_id) -- marca como publicado
t4: COMMIT TX
t5: kafkaTemplate.send(settlementEvent)  -- 💥 proceso muere ACÁ
```

→ la orden quedó FILLED, la marca dice "publicado", pero Kafka nunca
recibió el mensaje. Pérdida silenciosa permanente, sin ninguna señal de
que algo falló.

El Outbox cierra esta ventana porque el "compromiso de publicar" (fila
en `outbox`) se persiste atómicamente junto con el cambio de estado —
nunca hay un instante donde la orden esté FILLED sin que el compromiso
de publicar también exista. El publish real a Kafka queda a cargo de un
proceso que puede reintentar sin arriesgar esa atomicidad.

---

## 4. Casos de aceptación

Cada uno debe estar cubierto por al menos un test de integración
(Testcontainers, Postgres + Kafka reales) y ser reproducible manualmente
vía el endpoint de seed.

### 4.1 Idempotencia (`IdempotencyTest`)
- ER duplicado (mismo `secondaryTradeId`+`operationNumber`, distinto
  `fixId`) no duplica fila en el ledger ni incrementa `executionsCount`
  dos veces.
- ER con mismo `fixId` pero distinta clave de dedup SÍ se aplica (prueba
  negativa de que `fixId` no es la clave real).

### 4.2 Transición de estado inválida (`InvalidStateTransitionTest`)
- ER legítimamente nuevo (clave de dedup nunca vista) sobre una orden ya
  `FILLED` se rechaza, la orden no se modifica.
- Idem sobre orden `CANCELLED`.

### 4.3 Creación de orden — invariante I1 (cubrir si no existe todavía)
- Primer ER visto para una orden inexistente con status `NEW` crea la
  orden correctamente.
- Primer ER visto para una orden inexistente con status distinto de
  `NEW` se rechaza como error permanente (violación de secuencia — ver
  Decisión D5).

**Decisión D5**: aunque el PDF garantiza entrega en orden y que toda
orden arranca con NEW, el servicio valida esta invariante explícitamente
en el punto de creación en vez de asumirla sin control. Un ER no-NEW
para una orden inexistente se trata como error permanente, no se infiere
un NEW implícito con datos de un ER que no es el fundacional.

### 4.4 Secuencia intercalada (`InterleavedSequenceTest`)
- ER de dos órdenes distintas intercalados (A, B, A, B, A, B) resultan
  en que cada orden refleja fielmente su propia secuencia — status final
  correcto, `executionsCount` correcto, ledger de cada orden solo
  contiene sus propios ER en el orden de inserción correcto.

### 4.5 Settlement único (`SettlementUniquenessTest`)
- Reentrega del ER que dispara `FILLED` no duplica la fila en `outbox`.
- Orden que termina `CANCELLED` no genera fila en `outbox`.

### 4.6 Resiliencia ante caída de instancia (manual, documentado en README)
- Con una instancia caída, sus particiones asignadas quedan sin consumir
  (o son reasignadas por rebalanceo de Kafka al detectar la caída —
  ambos comportamientos son válidos y se documentan).
- Al reiniciar la instancia caída, retoma sin duplicar ni perder ER.

### 4.7 (Opcional, si el tiempo alcanza) Concurrencia real
- Dos threads aplicando ER de la misma orden en simultáneo no corrompen
  el estado (verifica que el lock pesimista realmente serializa el
  acceso, no solo en el caso feliz sin contención).

### 4.8 Manejo de errores y DLQ (`ExecutionReportListenerTest`)

**Agregado tras revisión post-implementación**: los casos 4.1 a 4.7
ejercitan `OrderProcessingService` directamente, sin pasar por
`ExecutionReportListener` — la capa que decide el ruteo a DLQ según
tipo de excepción (spec §2 G5). Verificado manualmente contra Kafka
real, pero faltaba cobertura automatizada. Se agrega como caso
explícito para no depender solo de verificación manual:

- Un ER con campos obligatorios faltantes (`PermanentProcessingException`)
  se publica a `er.raw.dlq` y no llega a crear/modificar ninguna orden.
- Un ER válido en forma pero con transición de estado inválida
  (`InvalidStateTransitionException`) se publica a `er.raw.dlq` sin
  modificar la orden existente.
- En ambos casos, el offset del mensaje original se confirma (ack) —
  no debe reintentarse indefinidamente.

### 4.9 Exclusión mutua entre los dos pollers (`OutboxClaimTest`)

**Agregado tras revisión post-implementación**: el caso 4.5 cubría la
unicidad del *insert* en `outbox`, pero nada ejercitaba el reparto de
filas entre los dos pollers que corren en paralelo. Es la parte del
diseño donde la garantía de "un settlement por orden" se sostiene o se
rompe (ver §2 G6).

- `claim` devuelve la fila y le escribe `locked_at`.
- Un segundo `claim` —representando el SELECT del otro poller,
  arrancando **después** de que el primero ya commiteó— **no** ve la fila
  ya reclamada. Esta es la regresión que producía los duplicados.
- Un claim vigente no vuelve a ser reclamable; uno vencido
  (`locked_at` más viejo que el umbral) sí, cubriendo la caída de un
  poller a mitad de publish.
- `releaseClaim` devuelve la fila al pool sin esperar el vencimiento
  (camino del publish fallido).
- `markAsPublished` saca la fila del pool y limpia el claim.
- El batch devuelto viene ordenado por `id`.

El test es un **regression test** verificado: al quitar el filtro de
`locked_at` de la query de claim, los dos casos de exclusión fallan
exactamente en el punto esperado.

**Nota de aislamiento**: el poller se desactiva en el perfil de test
(`app.outbox.poller-enabled=false`) porque competiría por las mismas
filas y las marcaría `published` antes de que el test pueda reclamarlas.

---

## 5. Distinción: idempotencia de negocio vs. de transporte

Dos capas distintas, ambas necesarias, ninguna sustituye a la otra:

| | Idempotencia de negocio | Idempotencia de transporte |
|---|---|---|
| Mecanismo | `UNIQUE` constraint + `ON CONFLICT DO NOTHING` | `enable.idempotence=true` en Kafka producer |
| Protege contra | Tu lógica aplicando el mismo ER/settlement dos veces | Reintento de red del producer duplicando el mensaje en el topic |
| Nivel | Aplicación / DB | Transporte / Kafka |

---

## 6. Alcance explícitamente fuera de foco

Conforme al PDF ("no construyas: auth, UI real, features extra, ni
hardening de producción"):

- No hay autenticación/autorización en los endpoints.
- No se implementa el consumidor del topic `settlement` (el PDF dice
  explícitamente que no hace falta).
- No hay UI.
- No se versiona el DDL con migraciones incrementales (V2, V3...) — se
  edita `V1` directamente durante la fase de diseño activo del esquema,
  aceptable para el alcance de este challenge, no para producción.