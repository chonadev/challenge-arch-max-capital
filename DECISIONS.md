# DECISIONS.md

Este documento justifica las decisiones de diseño del challenge. Se apoya
en `SPEC.md`, que contiene el detalle completo de invariantes, garantías
y casos de aceptación — acá se resume lo que el PDF pide explícitamente
cubrir.

---

## 1. Elección de broker y por qué

**Kafka**, sobre RabbitMQ. El challenge exige orden estricto intra-orden
y paralelismo entre órdenes distintas, con dos consumidores. El
particionado de Kafka por key resuelve esto nativamente: mismo
`numericOrderId` como key del mensaje → misma partición siempre (hash
determinístico) → orden garantizado dentro de esa partición sin construir
ningún mecanismo de coordinación propio. RabbitMQ requeriría un
consistent-hash exchange o colas por hash de orden para lograr algo
equivalente — más piezas para el mismo resultado.

**6 particiones**: se reparten parejo entre 2 instancias (3 c/u) y dejan
margen de escalado. No hay un cálculo más fino detrás — es una elección
razonable, no derivada de un requisito numérico del PDF.

## 2. Estrategia de secuencia por orden con 2 consumidores, y qué se resigna

Particionado por `numericOrderId`, ambas instancias en el mismo consumer
group — Kafka asigna particiones completas a cada miembro, nunca reparte
una partición entre dos consumidores del mismo grupo.

**Segunda línea de defensa**: `SELECT ... FOR UPDATE` (lock pesimista)
sobre la fila de la orden antes de aplicar cualquier ER. El particionado
cubre el caso normal; el lock cubre la ventana breve de un rebalanceo de
consumer group donde en teoría podría haber superposición.

**Qué se resigna**: no hay garantía de orden entre órdenes que comparten
partición — no hace falta, el PDF exige orden solo intra-orden. Tampoco
se implementó backoff configurable de reintentos (ver punto 6).

## 3. Estrategia de idempotencia y clave de dedup

**Clave de dedup**: `(secondaryTradeId, operationNumber)`, no `fixId`. El
PDF marca explícitamente esos dos campos con el comentario `// identidad
de la ejecución (para dedup)` en la estructura del ER — no dice
literalmente "usá este campo", pero es la pista más fuerte del documento,
más fuerte que la posición de `fixId` en la lista de campos.

**Mecanismo**: `UNIQUE(secondary_trade_id, operation_number)` en
`execution_ledger` + `INSERT ... ON CONFLICT DO NOTHING`. Atómico: no hay
ventana entre "chequear si existe" e "insertar" donde otro proceso se
cuele.

**Complementario, no sustituto**: `enable.idempotence=true` en el
producer de Kafka resuelve un problema distinto — duplicado por reintento
de red del propio producer, no duplicado de negocio. Se necesitan ambos:
uno protege la capa de aplicación, el otro la capa de transporte.

## 4. Cómo se persiste el estado y por qué ese motor

**Postgres**, por dos capacidades nativas que el diseño necesita:

1. Transacciones ACID con `SELECT ... FOR UPDATE` multi-tabla: en una
   sola transacción se toma el lock, se inserta en el ledger, se
   actualiza la orden, y si corresponde se inserta en el outbox.
2. `UNIQUE` + `ON CONFLICT DO NOTHING` como mecanismo de idempotencia a
   nivel DB, no solo aplicación.

Un motor NoSQL daría lo segundo sin esfuerzo, pero no lo primero.

## 5. Cómo se garantiza que la orden refleje exactamente su secuencia de ER

El estado no se sobreescribe con el último ER — se computa a partir del
estado ya persistido más el ER entrante, dentro de la transacción que
sostiene el lock. La transición de `status` se valida contra el status
ya guardado (`Order.applyExecutionReport()`); `executionsCount` se
incrementa sobre el valor anterior.

Un ER no puede aplicarse sobre una orden en estado terminal (`FILLED` /
`CANCELLED`) — se valida explícitamente y se rechaza con
`InvalidStateTransitionException`.

**Invariante adicional no explícita en el PDF pero necesaria**: el primer
ER visto para una orden inexistente debe ser `NEW`. Si no lo es, se
rechaza como error permanente en vez de crear la orden implícitamente
con datos de un ER que no debería ser el fundacional — evita que un
mensaje corrupto o fuera de secuencia (por una falla ajena a la garantía
de orden del broker) contamine el estado inicial de una orden.

## 6. Política de errores de procesamiento y reintentos

**Taxonomía**:
- **Permanente** (no reintentable): JSON malformado, texto plano no JSON, campos obligatorios
  faltantes, o transición de estado inválida (`PermanentProcessingException`, `InvalidStateTransitionException`). 
  Se excluyen de los reintentos (`exclude`), registrando un `log.error` inmediato y derivándolos directamente a `er.raw.dlq` sin bloquear el flujo de la partición.
- **Transitorio** (reintentable): fallo de conexión a DB, timeout de red. 
  Se gestionan automáticamente mediante `@RetryableTopic` con reintentos configurados (3 intentos con backoff exponencial) y, si se agotan, se envían a la DLQ.

**Mecanismo de Deserialización Segura**:
- Uso de `ErrorHandlingDeserializer` en `KafkaConsumerConfig` para envolver key y value. 
  Si se recibe un mensaje con formato corrupto o texto plano (poison pill), no se entra en bucle infinito de reintentos a nivel de consumidor; el deserializador produce un valor `null` que la validación detecta y rutea como error permanente hacia la DLQ.

**Cómo se evita perder o duplicar un ER**: `ack-mode: record` (gestionado por Spring Kafka y `@RetryableTopic`). El offset solo se confirma tras completar el procesamiento con éxito o al derivar el mensaje a DLQ. La idempotencia del ledger absorbe cualquier reentrega.

## 7. Cómo se garantiza que el settlement se emita una sola vez

**Por qué no alcanza con publicar directo al detectar FILLED**: es un
problema de dual-write. Si se actualiza la orden y después se publica a
Kafka como dos pasos separados, un crash entre ambos pasos pierde el
evento silenciosamente — la orden queda FILLED pero el settlement nunca
sale. Ver `SPEC.md` §3, Decisión D4, para el análisis con línea de
tiempo completa.

**Mecanismo — Outbox Pattern**:
1. Al aplicar el ER que lleva la orden a FILLED, se inserta una fila en
   `outbox` en la misma transacción que el update de la orden —
   atomicidad DB-vs-DB, trivial de lograr.
2. Un poller separado (`@Scheduled`, cada 500ms) lee filas pendientes con
   `SELECT ... FOR UPDATE SKIP LOCKED` y las publica.
3. Solo tras confirmar el ack del broker (`.get()` bloqueante), se marca
   `published=true`.

`SKIP LOCKED` permite que las dos instancias corran su propio poller en
paralelo sin coordinación explícita ni pisarse — cada una toma un
subconjunto de filas.

`UNIQUE(numeric_order_id, event_type)` en `outbox` es la defensa
adicional contra doble insert por la misma orden.

`CANCELLED` no emite settlement — regla explícita del PDF, implementada
como condición simple (`if newStatus == FILLED`) antes del insert al
outbox.

## 8. Trade-offs dejados afuera a propósito

- **Consumidor del topic `settlement`**: no se implementa, conforme al
  PDF ("no hace falta implementar el consumidor de ese settlement").
- **Migraciones incrementales de DDL** (V2, V3...): se editó `V1`
  directamente durante la fase de diseño activo del esquema. Válido para
  el alcance de este challenge, no para un entorno productivo con datos
  reales que no se puedan recrear.
- **Debezium/CDC** en vez de poller manual para el outbox: el poller
  `@Scheduled` es más simple de levantar en el tiempo del challenge; CDC
  sería la elección en una versión productiva, por menor latencia y no
  depender de polling activo.
- **Auth, UI, hardening de producción**: fuera de alcance según el PDF.

## Supuestos asumidos

- La clave de dedup es `(secondaryTradeId, operationNumber)`, no
  explícita al 100% en el PDF pero la lectura más fiel al texto (ver
  punto 3).
- El primer ER de una orden nueva debe ser `NEW`; si no lo es, se
  rechaza en vez de inferir la creación silenciosamente (ver punto 5).
- El número de particiones de Kafka (6) es una elección de diseño propia,
  no derivada de un requisito del PDF.