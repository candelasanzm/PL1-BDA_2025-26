---------- Cuestión 1

-- Creamos la tabla
CREATE TABLE estudiantes (
    estudiante_id SERIAL PRIMARY KEY ,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT
);

-- Insertamos en la tabla los datos creados con el archivo estudiantes.py
COPY estudiantes(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

---------- Cuestión 2

-- Consultamos el tamaño del bloque
SHOW block_size;

-- Consulta tamaño real en bloques
SELECT
  pg_relation_size('public.estudiantes') AS tamaño_tabla,
  pg_relation_size('public.estudiantes') / 8192 AS bloques_reales;

-- Cuántos registros caben realmente
SELECT
  COUNT(*) AS num_registros,
  COUNT(*) / (pg_relation_size('public.estudiantes') / 8192)
    AS factor_bloque_medio_real
FROM public.estudiantes;

---------- Cuestión 3

-- Muestra los estudiantes con índice 500
SELECT * FROM public.estudiantes WHERE indice = 500;

-- Devuelve el número de tuplas
SELECT COUNT(*) AS tuplas FROM public.estudiantes WHERE indice = 500;

-- Devuelve los bloques que Postgres lee del disco
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM public.estudiantes
WHERE indice = 500;

-- Obtener las estadísticas de la tabla y sus campos
ANALYZE public.estudiantes;

SELECT
  relname,
  seq_scan, seq_tup_read,
  idx_scan, idx_tup_fetch,
  n_live_tup
FROM pg_stat_user_tables
WHERE relname = 'estudiantes';

SELECT
  relname,
  heap_blks_read,
  heap_blks_hit
FROM pg_statio_user_tables
WHERE relname = 'estudiantes';

SELECT
  attname,
  n_distinct,
  most_common_vals,
  most_common_freqs
FROM pg_stats
WHERE schemaname = 'public'
  AND tablename = 'estudiantes'
  AND attname = 'indice';

---------- Cuestión 4

-- Vuelvo a repetir la Cuestión 3
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM public.estudiantes
WHERE indice = 500;

---------- Cuestión 5

-- Creo una tabla estudiantes2
CREATE TABLE estudiantes2 (
    estudiante_id SERIAL PRIMARY KEY,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT
);

-- Insertamos en la tabla los datos creados con el archivo estudiantes.py
COPY estudiantes2(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

-- Creamos un índice
CREATE INDEX idx_estudiantes2_indice ON public.estudiantes2(indice);

-- Ejecutamos el clustering (para ordenar físicamente)
CLUSTER public.estudiantes2 USING idx_estudiantes2_indice;

-- Actualiza las estadísticas
ANALYZE public.estudiantes2;

-- Veamos cuántos bloques ocupa ahora
SELECT pg_relation_size('public.estudiantes2') / 8192 AS bloques_reales;

---------- Cuestión 6

-- Muestra los estudiantes con índice 500
SELECT * FROM public.estudiantes2 WHERE indice = 500;

-- Devuelve el número de tuplas
SELECT COUNT(*) AS tuplas FROM public.estudiantes2 WHERE indice = 500;

-- Devuelve los bloques que Postgres lee del disco
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM public.estudiantes2
WHERE indice = 500;

---------- Cuestión 7

-- Borrar 5000000 de tuplas
DELETE FROM public.estudiantes
WHERE estudiante_id IN (
  SELECT estudiante_id
  FROM public.estudiantes
  ORDER BY random()
  LIMIT 5000000
);

-- Comprobar el tamaño
SELECT pg_relation_size('public.estudiantes') / 8192 AS bloques;

---------- Cuestión 8

-- Insertar estudiante
INSERT INTO public.estudiantes(nombre, codigo_carrera, edad, indice) VALUES ('Cuestion8', 3, 20, 5);

---------- Cuestión 9

-- Vacuum (Analyze), limpia tuplas muertas marcando el espacio como reutilizable, pero no reduce mucho el espacio físico
VACUUM (ANALYZE) public.estudiantes;

-- Vacuum Full recupera el máximo espacio
VACUUM FULL public.estudiantes;
ANALYZE public.estudiantes;

-- Reactualizar los índices
REINDEX TABLE estudiantes;

-- Medir cuánto espacio se ha recuperado
SELECT
  pg_relation_size('public.estudiantes') AS tabla,
  pg_indexes_size('public.estudiantes')  AS indices,
  pg_total_relation_size('public.estudiantes') AS total;

---------- Cuestión 10

-- Crear nueva tabla estudiantes3
CREATE TABLE estudiantes3 (
    estudiante_id SERIAL,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT,
    h INT GENERATED ALWAYS AS (codigo_carrera % 20) STORED,
    PRIMARY KEY (estudiante_id, h)
) PARTITION BY LIST (h);