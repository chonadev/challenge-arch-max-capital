# Order Processing Service

Servicio de procesamiento de ExecutionReports (ER) para Max Capital —
challenge técnico. Ver `SPEC.md` para la especificación funcional
completa y `DECISIONS.md` para la justificación de cada decisión de
diseño.

## Cómo correrlo

```bash
docker compose up -d --build
```

Levanta: Postgres, Kafka (KRaft), inicialización de topics, y 2
instancias del servicio (`localhost:8081` y `localhost:8082`).

Si ya habías corrido el proyecto antes y cambiaste el DDL, limpiá el
volumen primero (ver sección de troubleshooting).

## Endpoints

- `GET /orders/{numericOrderId}` — estado actual de la orden + ledger
- `POST /test/seed/{scenario}` — dispara un escenario de prueba a `er.raw`

## Escenarios de prueba

### 1. ER intercalados entre dos órdenes (`interleaved`)

```bash
curl -X POST http://localhost:8081/test/seed/interleaved
curl http://localhost:8081/orders/1001   # debería terminar en FILLED
curl http://localhost:8081/orders/2001   # debería terminar en CANCELLED, sin settlement
```

### 2. ER duplicado / reentrega (`duplicates`)

```bash
curl -X POST http://localhost:8081/test/seed/duplicates
curl http://localhost:8081/orders/3001
# executionsCount debe ser 2, no 3, aunque se enviaron 3 ER
# (el segundo PARTIALLY_FILLED está repetido a propósito)
```

### 3. Caída de una instancia a mitad de proceso

```bash
docker compose stop order-service-1
curl -X POST http://localhost:8082/test/seed/bulk_crash_test
# esperar unos segundos
docker exec -it $(docker ps -qf "name=postgres") psql -U orders_user -d orders \
  -c "SELECT numeric_order_id, status, executions_count FROM orders ORDER BY numeric_order_id;"
# algunas órdenes pueden quedar incompletas o directamente sin fila,
# dependiendo de si Kafka ya rebalanceó las particiones de la instancia caída

docker compose start order-service-1
# esperar unos segundos y repetir el SELECT: todas las órdenes deben
# quedar completas (FILLED), ninguna con executions_count duplicado
```

Nota: Kafka puede rebalancear las particiones de la instancia caída
hacia la instancia viva automáticamente (comportamiento esperado y
correcto — ver `DECISIONS.md` punto 2). Si eso ocurre antes de que el
seed llegue, es posible que la instancia viva absorba todo el trabajo
sin dejar nada pendiente para cuando la caída se reinicie; el
comportamiento sigue siendo correcto en ambos casos.

## Tests automatizados

```bash
mvn test
```

Usa Testcontainers (Postgres + Kafka reales, no mocks) — requiere Docker
corriendo, pero no requiere que `docker compose up` esté levantado (los
containers de test son efímeros e independientes).

Cobertura: idempotencia, transición de estado inválida, secuencia
intercalada, settlement único, creación de orden con validación de
invariante. Ver `SPEC.md` §4 para el detalle de cada caso de aceptación.

## Troubleshooting

**Error de Flyway (`Migration checksum mismatch`)**: pasa si el DDL
cambió después de que la base ya corrió una versión anterior de la
migración. Solución en desarrollo:

```bash
docker compose down -v
docker compose up -d --build
```

## Estructura del repo

- `SPEC.md` — especificación funcional (fuente de verdad del diseño)
- `DECISIONS.md` — justificación de decisiones, siguiendo los 8 puntos
  pedidos por el enunciado
- `src/main/resources/db/migration/` — DDL vía Flyway
- `src/main/resources/scenarios/` — datos de los escenarios de prueba
- `src/test/` — tests de integración vía Testcontainers