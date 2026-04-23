# Informe — TP Coordinación

En este informe se detallan las decisiones de diseño y coordinación implementadas, poniendo el  foco en cómo se resolvió la sincronización entre Sum, Aggregation y Join para permitir escalabilidad con múltiples clientes y múltiples instancias de controles.

El diseño parte de los supuestos del trabajo práctico:

- No hay fallas de procesos ni de red.
- No hay pérdida ni reordenamiento de mensajes.
- No se requiere persistencia ni recuperación de estado tras caídas, ya que no van a ocurrir.

---

## 1. Aislamiento de flujos por cliente

En el esqueleto presentado, no se contemplaba la concurrencia de múltiples clientes. Es decir, ocurría una mezcla de estados entre consultas, lo que impedía procesar varias al mismo tiempo.

Para resolver esto, el sistema propaga un client_id en todo el pipeline interno para evitar mezcla de estados entre consultas concurrentes.

- En Gateway, cada conexión genera un UUID y lo adjunta a DataMessage y EOFMessage.
- En Sum, Aggregation y Join, el estado se indexa por client_id.
- En el resultado final, Join conserva client_id y Gateway entrega la respuesta únicamente al handler correspondiente.

Esta decisión permite procesar clientes en paralelo sin interferencia cruzada. Además, al cerrar una consulta se elimina su estado local en cada control para mantener uso de memoria acotado.

---

## 2. Coordinación de cierre en Sum

Las instancias de Sum consumen mensajes de una cola compartida. El Gateway envía únicamente un EOF por cliente, que llega a una sola instancia de Sum. Por lo tanto, se necesita un mecanismo de coordinación para que todas las instancias de Sum sepan cuándo han terminado de procesar los datos de ese cliente y puedan enviarlos a los Aggregators.

En primera instancia, se descartaron soluciones que implicaban modificar como el Gateway envía los datos, como por ejemplo enviar un EOF a cada instancia de Sum, ya que esto iría en contra de la consigna que establece límites claros sobre la mutabilidad del Gateway.

Luego, se consideró la posibilidad de tener un anillo lógico entre las instancias de Sum para difundir el EOF, pero se descartó ya que ver el EOF en esta cola no implica que la instancia haya terminado de procesar los datos, sino que podría recibir mensajes posteriores al EOF debido a la naturaleza asíncrona del sistema.

Finalmente, se decidió implementar una barrera con coordinador dinámico por cliente:

1. El Gateway envía el EOF asociado a un client_id con el número total de registros enviados (total_records).
2. El Sum que recibe ese EOF asume el rol de coordinador y difunde SumEOFNoticeMessage, que incluye client_id, total_records y su identificador de instancia (coordinator_id).
3. Cada Sum reporta al coordinador el número de registros procesados para ese cliente (processed_count) mediante SumCountUpdateMessage y entra en estado closing para ese cliente.
4. Mientras esté en estado closing, cada vez que recibe un mensaje de datos para ese cliente, actualiza processed_count y reporta al coordinador con SumCountUpdateMessage.
5. El coordinador acumula el último conteo por worker y, cuando la suma global alcanza total_records, difunde SumFlushMessage.
6. Cada Sum flushea sus parciales hacia Aggregation y emite un EOF para ese cliente hacia cada Aggregator.

Consideraciones:
- Por los supuestos de no caídas ni pérdida de mensajes, no se implementan timeouts ni mecanismos de recuperación ante fallas.
- El coordinador se elige dinámicamente por cliente, lo que permite balancear la carga de coordinación entre las instancias de Sum.
- El coordinador solo mantiene el conteo acumulado por cliente, sin necesidad de almacenar detalles de los mensajes individuales.

---

## 3. Coordinación en Aggregation y particionamiento consistente

Para evitar procesamiento redundante, los datos no se envían en broadcast a todos los Aggregators. En su lugar, cada fruta se enruta a una sola partición usando hash determinístico:

```python
def aggregation_id_for(fruit):
    digest = hashlib.md5(fruit.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % config.AGGREGATION_AMOUNT
```

Con esto, se tiene que:

- Todas las frutas van a un solo Aggregator, evitando duplicaciones.
- No se necesita conocer las frutas posibles de antemano, ya que el hash es dinámico.

El EOF sí se difunde a todos los Aggregators. Cada instancia espera recibir un EOF por cada sum (en total SUM_AMOUNT EOFs) por cliente antes de emitir su top parcial; así garantiza que ya recibió todos los aportes de Sum para su partición.

---

## 4. Consolidación de resultados en Join

En el código base, Join se quedaba con el primer top que recibía del Aggregator, y lo reenviaba al Gateway. Esto no es correcto cuando AGGREGATION_AMOUNT > 1, ya que cada Aggregator envía un top parcial y Join debe consolidar esos parciales para obtener el top global por cliente.

Para que sea escalable, para la consolidación se mantiene un min-heap de tamaño TOP_SIZE con los mejores resultados recibidos hasta el momento. Cada vez que llega un top parcial de un Aggregator, se itera sobre sus frutas y cantidades, y se actualiza el heap:

```python
for fruit, amount in partial_top:
    if len(heap) < TOP_SIZE:
        heapq.heappush(heap, (amount, fruit))
    elif amount > heap[0][0]:
        heapq.heapreplace(heap, (amount, fruit))
```

Cuando Join recibe AGGREGATION_AMOUNT parciales para un cliente, guarda los elementos del heap en orden descendente y los envía al Gateway como resultado final.

Esto permite obtener el top global sin necesidad de almacenar todos los resultados intermedios, manteniendo memoria acotada y un costo de inserción eficiente.

---

## 5. Cierre limpio con SIGTERM

Se implementó graceful shutdown en Sum, Aggregators y Join, registrando un handler que detiene el consumo de mensajes sin corromper el estado de la conexión. Al recibir la señal, se encola un callback mediante `add_callback_threadsafe()`, asegurando que la detención ocurra en un punto seguro. Esto permite que todas las conexiones cierren correctamente y se limpie el estado de los canales.

En Aggregation y Join, al ser single-threaded, el handler se registra antes de iniciar el consumo y es idempotente. En Sum, data plane y control plane corren en threads separados, cada uno con su propio loop pika. Se detienen ambos llamando a `add_callback_threadsafe()` sobre sus conexiones respectivas. El main thread aguarda `control_thread.join()` en el finally para garantizar que ambos loops terminaron completamente antes de cerrar las conexiones. Por el supuesto de no caídas, no se implementan timeouts ni mecanismos de recuperación ante fallas durante el shutdown.

---

## 6. Escalabilidad

La escalabilidad de este sistema tiene dos dimensiones: múltiples clientes concurrentes y múltiples instancias de controles (Sum, Aggregators, Join).

### Múltiples clientes concurrentes

Al indexar todo estado por client_id desde Gateway hasta Join, los clientes no interfieren entre sí. Pueden solaparse sus consultas sin mezcla de resultados ni condiciones de carrera: cada uno mantiene su propio estado en Sum, sus propios diccionarios en Aggregators, su propio heap en Join.

Lo importante es que al finalizar cada consulta, se limpian los estados locales. Con el cleanup, un cliente que termina libera todo lo que consumió.

### Múltiples instancias de controles

Todos los Sum comparten la misma cola de entrada, distribuyendo la carga de procesamiento. Una decisión que no me termina de convencer del todo es que el `ControlPublisher` queda duplicado entre data plane y control plane dentro de cada Sum. En la práctica, esto implica dos conexiones pika para publicar sobre el canal de control, cuando conceptualmente podría pensarse una sola conexión compartida.

Esa alternativa compartida no se usó porque pika no es thread-safe. Por eso se priorizó aislamiento por thread, cada plano con su propia conexión.

La otra opción era agregar un tercer thread por instancia de Sum, con una cola interna, y que ese thread fuera el único publicador al canal de control. Sin embargo, el costo por agregar una capa adicional de threads y colas internas se consideró mayor que la simplicidad de tener dos conexiones independientes. 

En Aggregation, el particionamiento por hash resuelve el problema de escala. Cada fruta va siempre al mismo Aggregator; no hay coordinación entre ellas. Internamente cada Aggregator acumula frutas en un diccionario y ordena una única vez al cierre.

El Join es único y recibe los tops parciales de cada Aggregator. La consolidación se hace localmente en un heap, sin necesidad de comunicación entre instancias. Si fuera necesario escalar Join, se requeriría particionar por client_id y enrutar cada resultado a su partición.
