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
CREATE TABLE public.estudiantes3 (
  estudiante_id SERIAL,
  nombre TEXT,
  codigo_carrera INT,
  edad INT,
  indice INT,
  PRIMARY KEY (estudiante_id, codigo_carrera)
) PARTITION BY HASH (codigo_carrera);

-- Hago las 20 particiones
CREATE TABLE estudiantes3_p0 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 0);
CREATE TABLE estudiantes3_p1 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 1);
CREATE TABLE estudiantes3_p2 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 2);
CREATE TABLE estudiantes3_p3 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 3);
CREATE TABLE estudiantes3_p4 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 4);
CREATE TABLE estudiantes3_p5 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 5);
CREATE TABLE estudiantes3_p6 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 6);
CREATE TABLE estudiantes3_p7 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 7);
CREATE TABLE estudiantes3_p8 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 8);
CREATE TABLE estudiantes3_p9 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 9);
CREATE TABLE estudiantes3_p10 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 10);
CREATE TABLE estudiantes3_p11 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 11);
CREATE TABLE estudiantes3_p12 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 12);
CREATE TABLE estudiantes3_p13 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 13);
CREATE TABLE estudiantes3_p14 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 14);
CREATE TABLE estudiantes3_p15 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 15);
CREATE TABLE estudiantes3_p16 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 16);
CREATE TABLE estudiantes3_p17 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 17);
CREATE TABLE estudiantes3_p18 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 18);
CREATE TABLE estudiantes3_p19 PARTITION OF estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 19);

-- Rellenamos la tabla
COPY estudiantes3(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

-- Cuántos bloques ocupa cada partición
SELECT pg_relation_size('public.estudiantes3_p0') / 8192 AS bloques_p0;
SELECT pg_relation_size('public.estudiantes3_p1') / 8192 AS bloques_p1;
SELECT pg_relation_size('public.estudiantes3_p2') / 8192 AS bloques_p2;
SELECT pg_relation_size('public.estudiantes3_p3') / 8192 AS bloques_p3;
SELECT pg_relation_size('public.estudiantes3_p4') / 8192 AS bloques_p4;
SELECT pg_relation_size('public.estudiantes3_p5') / 8192 AS bloques_p5;
SELECT pg_relation_size('public.estudiantes3_p6') / 8192 AS bloques_p6;
SELECT pg_relation_size('public.estudiantes3_p7') / 8192 AS bloques_p7;
SELECT pg_relation_size('public.estudiantes3_p8') / 8192 AS bloques_p8;
SELECT pg_relation_size('public.estudiantes3_p9') / 8192 AS bloques_p9;
SELECT pg_relation_size('public.estudiantes3_p10') / 8192 AS bloques_p10;
SELECT pg_relation_size('public.estudiantes3_p11') / 8192 AS bloques_p11;
SELECT pg_relation_size('public.estudiantes3_p12') / 8192 AS bloques_p12;
SELECT pg_relation_size('public.estudiantes3_p13') / 8192 AS bloques_p13;
SELECT pg_relation_size('public.estudiantes3_p14') / 8192 AS bloques_p14;
SELECT pg_relation_size('public.estudiantes3_p15') / 8192 AS bloques_p15;
SELECT pg_relation_size('public.estudiantes3_p16') / 8192 AS bloques_p16;
SELECT pg_relation_size('public.estudiantes3_p17') / 8192 AS bloques_p17;
SELECT pg_relation_size('public.estudiantes3_p18') / 8192 AS bloques_p18;
SELECT pg_relation_size('public.estudiantes3_p19') / 8192 AS bloques_p19;

-- Hago la suma de todas las particiones
SELECT (pg_relation_size('public.estudiantes3_p0') +
        pg_relation_size('public.estudiantes3_p1') +
        pg_relation_size('public.estudiantes3_p2') +
        pg_relation_size('public.estudiantes3_p3') +
        pg_relation_size('public.estudiantes3_p4') +
        pg_relation_size('public.estudiantes3_p5') +
        pg_relation_size('public.estudiantes3_p6') +
        pg_relation_size('public.estudiantes3_p7') +
        pg_relation_size('public.estudiantes3_p8') +
        pg_relation_size('public.estudiantes3_p9') +
        pg_relation_size('public.estudiantes3_p10') +
        pg_relation_size('public.estudiantes3_p11') +
        pg_relation_size('public.estudiantes3_p12') +
        pg_relation_size('public.estudiantes3_p13') +
        pg_relation_size('public.estudiantes3_p14') +
        pg_relation_size('public.estudiantes3_p15') +
        pg_relation_size('public.estudiantes3_p16') +
        pg_relation_size('public.estudiantes3_p17') +
        pg_relation_size('public.estudiantes3_p18') +
        pg_relation_size('public.estudiantes3_p19')) / 8192 AS bloques_totales;

---------- Cuestión 11