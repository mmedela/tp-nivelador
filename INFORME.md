# TP Nivelador

El sistema distribuido consta de un **servidor central** (Python) que emula la central de Loteria Nacional y varios **clientes** (Go) que representan agencias. Cada agencia envia sus apuestas en _batches_, el servidor las persiste, espera a que un querum de agencias termine, realiza el sorte y devuelve los ganadores a cada agencia.

---

## 1. Protocolo de comunicacion

### 1.1 Framing

Todos los mensajes usan el mismo formato de trama (length-prefixed):
```
+-------------------------------+---------------------------+
| Length (4 bytes big-endian)   | payload (length bytes)    |
|                               |                           |
+-------------------------------+---------------------------+
                                | tag | data                |
                                | (1) | (length -1)         |
                                +---------------------------+
```
- `length`: tamaño del payload en bytes, bigendian, 4 octetos.
- `payload[0]`: _tag_ que identifica el mensaje
- `payload[1:0]`: datos del mensaje.

La lecutra se realiza en dos pasos: primero se leen los 4 bytes de longitud con `recv_all(sock, 4)` y luego el payload completo con `recv_all(sock, length)`. Esto evita lecturas parciales (short read), segun lo pedido por el ejercicio 4. 

### 1.2 Tags


|Tag        | Valor | Direccion             | Data                               |
------------|-------|-----------------------|------------------------------------|
|`AGENCY`   |   1   | Cliente -> servidor   | 4 bytes (agency_id)                |
|`BATCH`    |   2   | Cliente -> servidor   | apuestas en CSV separadas por `\n` |
|`FINISH`   |   3   | ambos                 | vacio                              |
|`WINNER`   |   4   | Servidor -> cliente   | 1 apuesta ganadora en CSV          |
|`BATCH_ACK`|   5   | Servidor -> cliente   | 1 byte: `0`=OK, `1`= FAIL          |

## 1.3 El formato de apuestas (CSV)

<first_name>, <last_name>,<document>,<birthdate>,<number>

Ejemplo: `A,B,00000000,2000-01-01,1234`

## 1.4 Flujo de comunicacion

1. el cliente envia primero su `Agency` y luego tantos `BATCH` como sean necesarios (de a `BATCH_SIZE` apuestas por mensaje). 
2. Por cada `BATCH` el servidor responde `BATCH_ACK` indicando exito o fracaso.
3. Al terminar, el cliente envia `FINISH`. El servidor registra la agencia en el conjunto de terminadas y espera (condvar) a alcanzar el quorum.
4. Alcanzado el quorum, el servidor calcula los ganadores (`bet.number == 1234`), filtra los de la agencia y los envia uno a uno con `WINNER`, cerrando con `FINISH`

---

## 2 serializacion

La serializacion es manual, sin librerias de alto nivel:

- **Apuestas**: CSV plano con campos separados por `,` y apuestas desntro de un batch separadas por `\n` (`deserialize_bets()`/`bytes,join("\n")`)
- **Longitudes y agency_id**: Enteros en big-endian (`int.to_bytes(4, "big")`/`encoding/binary..BigEndian`)
- **ACK**: un solo byte `0` o `1`

La separacion de responsabilidades se mantiene en una capa `protocol` independiente del modelo de dominio (`Bet`, `Lottery`) tanto en python comoen go.

---

## 3 Comunicaciones short read / short write

Las primiticas `recv_all` y `send_all`, en `safe_cockets` son tolerantes a lecturas/escrituras parciales. Condicion necesaria sobre sockets TCP_

- **recv_all(socket, size)** acumula en un buffer hasta leer `size` bytes. si `recv` devuelve `b''` (EOF, el peer cerro la conexion) Se lanza `OSError`, evitando un loop infinito

- **send_all(socket, size)** envia hasta escribir todos los bytes. Si `send` devuelve `0` se lanza `OSError`, evitando un loop infinito.

El analogo en el cliente de Go son `safe_socket.SendAll`/`RecvAll` con bucles de short-read/short-write sonre `io.Reader` / `io.Writer`.

---

## 4 Concurrencia y sincronizacion

El servidor es multi hilo. Existe un hilo por cliente (`_handle_cliente`). La sincronizacion se realiza con el siguiente mecanismo: 

| Mecanismo                  | Tipo                    | Protege / coordina                          |
|----------------------------|-------------------------|---------------------------------------------|
| `lottery_lock`             | `Lock`                  | acceso al archivo de apuestas (`store_bets`/`load_bets`) |
| `client_sockets_lock`      | `Lock`                  | conjunto `client_sockets` (alta/baja de sockets)        |
| `threads_lock`             | `Lock`                  | lista `clien_threads`                                  |
| `quorum_condition`         | `Condition`             | quórum de agencias terminadas (`finished_agencies`)    |
| `shutting_down`            | `Event`                 | señalización de shutdown al loop de accept y al wait   |

### 4.1 Quorum de agencias

Al recibir `FINISH`, el hilo del cliente agrega su `agency_id` a `finiched_agencies` bajo `quorum_condition`, hace `nofy_all()` y luego `wait()` mientras no se lance `AGENCY_QUORUM_MIN` y no se este cerrando. Al alcanzar el quorum, todos los hilos despiertan y proceden a calcular y enviar los ganadores de su agencia.

### 4.2 Protecicon de la loteria

`store_bets` y `load_bets` acceden al archivo CSV compartido, por lo que se protegen con `lottery_lock` para evitar interleaving y corrupcion.

---

## 5 Cierre _graceful_ (SIGTERM)

El cierre ordenado garantiza la liberacion de sockets, archivos y threads en un tiempo conocido y acotado.

### 5.1 Servidor

- `_handle_sigterm` se registra para `SIGTERM`: setea `shutting_down`, `nofy_all()` sobre la condvar (desbloquea el `wait()` del quorum) y  cierra el socket servidor y los sockets de clientes (shutdown + close). Esto desbloquea el accept y los `recv()` de los hilos.

- El loop de `accept()` chequea `shutting_down` y sale limpio.

- `_handle_client` ante una excepcion (una conexion cerrada, por ejemplo) la loguea y hace `return` (no hace raise porque los hilos hijos no propagan excepciones al padre). El bloque `finally` descarta el socket del set y lo cierra. 

- Los threads sib **daemon=False**, garantizando que el `finally` de cada hilo se ejecute y los recursos se eliminen deterministicamente

- `run()` hace `join(timeout=grace_time)` de los threads con un presupueste de tiempo (`GRACE_TIME`, default=4.0) qye se va descontando, acotando el tiempo total de cierre

### 5.2 Cliente

- Una go routine escucha `SIGTERM`, cancela el `context` y cierra la conexion, desbloqueando los `Recv`/`Send` del loop principal.

- El loop de lectura chequea `ctx.Done()` en cada iteracion y retorna limpio.

- `defer` asegura el cierre de `conn`, `inputFile` y `outputFile`

---

## 6 Procesamiento por _batches_

- El cliente lee `INPUT_FILE` linea a linea con `bufio.NewReader` (no carga el archivo completo en memoria) y agrupa hasta `BATCH_SIZE` lineas por mensaje.

- `BATCH_SIZE` se configura por variable de entorno

- El servidor deserializa el batch, persiste todas las apuesta bajo `lottery_lock` y responde un unico `BATCH_ACK` (OK solo si todo el batch se proceso correctamente)


---

## 7 Perfil de memoria

El cliente procesa la entrada de streaming (linea a linea, batch de a `BATCH_SIZE`), por lo que el conjunto de objetivos vivos es acotado y no crece con el tamaño del dataset.

Dado que en el runtime Go no dispara el GC hasta que el heap alcanza un floor de ~4MB, las asignaciones transitorias (strings por linea, copias, payload y frame por batch) inflabanel pico de memoria en datasets grandes. Se fuerza `runtime.GC()` cada n fluches de batch, lo que mantiene el pico de heap acotado e independiente del tamaño de la entrada, cumpliendo con el test de memory profile

---

## 8 Manejo de errores 

- **`recv_all`/`send_all`**: EOF o write cero → `OSError` (sin loop infinito).
- **`_handle_client`**: excepciones logueadas + `return`; `finally` libera socket.
- **`_handle_sigterm`**: `OSError` al cerrar sockets ignorados (`try/except`).
- **Cliente Go**: errores de send/recv logueados; `ctx.Err()` diferenciado para
  distinguir shutdown limpio de error real.