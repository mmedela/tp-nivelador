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
