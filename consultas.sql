---------- Cuestión 1

-- Creamos la tabla
CREATE TABLE IF NOT EXISTS public.estudiantes (
    estudiante_id SERIAL PRIMARY KEY ,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT
);

-- Insertamos en la tabla los datos creados con el archivo estudiantes.py
COPY public.estudiantes(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

-- Localizamos los ficheros de la tabla
SELECT pg_relation_filepath('public.estudiantes');

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
  COUNT(*) / (pg_relation_size('public.estudiantes') / 8192) AS factor_bloque_medio_real
FROM public.estudiantes;

---------- Cuestión 3

-- Muestra los estudiantes con índice 500
SELECT * FROM public.estudiantes WHERE indice = 500 ORDER BY estudiante_id;

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
CREATE TABLE IF NOT EXISTS public.estudiantes2 (
    estudiante_id SERIAL PRIMARY KEY,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT
);

-- Insertamos en la tabla los datos creados con el archivo estudiantes.py
COPY public.estudiantes2(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

DROP TABLE IF EXISTS public.estudiantes2_temp;

-- Reescribir la tabla ordenando por índice
CREATE TABLE public.estudiantes2_temp AS
SELECT *
FROM public.estudiantes2
ORDER BY indice, estudiante_id;

-- Vacio la tabla original y vuelvo a insertar en orden
TRUNCATE TABLE public.estudiantes2;

INSERT INTO public.estudiantes2(estudiante_id, nombre, codigo_carrera, edad, indice)
SELECT estudiante_id, nombre, codigo_carrera, edad, indice
FROM public.estudiantes2_temp
ORDER BY indice, estudiante_id;

-- Limpiar el temporal
DROP TABLE public.estudiantes2_temp;

-- Actualiza las estadísticas
ANALYZE public.estudiantes2;

-- Veamos cuántos bloques ocupa ahora
SELECT pg_relation_size('public.estudiantes2') / 8192 AS bloques_reales;

---------- Cuestión 6

-- Muestra los estudiantes con índice 500
SELECT * FROM public.estudiantes2 WHERE indice = 500 ORDER BY estudiante_id;

-- Devuelve el número de tuplas
SELECT COUNT(*) AS tuplas FROM public.estudiantes2 WHERE indice = 500;

-- Devuelve los bloques que Postgres lee del disco
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM public.estudiantes2
WHERE indice = 500
ORDER BY estudiante_id;

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
SELECT
  pg_relation_size('public.estudiantes')/8192 AS bloques_tabla,
  pg_indexes_size('public.estudiantes')/8192  AS bloques_indices,
  pg_total_relation_size('public.estudiantes')/8192 AS bloques_total;

---------- Cuestión 8

-- Insertar estudiante
INSERT INTO public.estudiantes(nombre, codigo_carrera, edad, indice)
VALUES ('Cuestion8', 3, 20, 5)
RETURNING ctid, estudiante_id;

---------- Cuestión 9

-- Vacuum (Analyze), limpia tuplas muertas marcando el espacio como reutilizable, pero no reduce mucho el espacio físico
VACUUM (ANALYZE) public.estudiantes;

-- Vacuum Full recupera el máximo espacio
VACUUM FULL public.estudiantes;
ANALYZE public.estudiantes;

-- Reactualizar los índices
REINDEX TABLE public.estudiantes;

-- Medir cuánto espacio se ha recuperado
SELECT
  pg_relation_size('public.estudiantes') AS tabla,
  pg_indexes_size('public.estudiantes')  AS indices,
  pg_total_relation_size('public.estudiantes') AS total;

---------- Cuestión 10

-- Crear nueva tabla estudiantes3
CREATE TABLE IF NOT EXISTS public.estudiantes3 (
  estudiante_id SERIAL,
  nombre TEXT,
  codigo_carrera INT,
  edad INT,
  indice INT,
  PRIMARY KEY (estudiante_id, codigo_carrera)
) PARTITION BY HASH (codigo_carrera);

-- Hago las 20 particiones
CREATE TABLE IF NOT EXISTS public.estudiantes3_p0 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 0);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p1 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 1);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p2 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 2);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p3 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 3);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p4 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 4);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p5 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 5);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p6 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 6);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p7 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 7);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p8 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 8);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p9 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 9);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p10 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 10);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p11 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 11);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p12 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 12);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p13 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 13);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p14 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 14);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p15 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 15);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p16 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 16);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p17 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 17);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p18 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 18);
CREATE TABLE IF NOT EXISTS public.estudiantes3_p19 PARTITION OF public.estudiantes3 FOR VALUES WITH (MODULUS 20, REMAINDER 19);

-- Rellenamos la tabla
COPY public.estudiantes3(nombre, codigo_carrera, edad, indice)
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

-- Actualizar estadisticas
ANALYZE public.estudiantes3;

-- Muestra los estudiantes con índice 500
SELECT * FROM public.estudiantes3 WHERE indice = 500;

-- Devuelve el número de tuplas
SELECT COUNT(*) AS tuplas FROM public.estudiantes3 WHERE indice = 500;

-- Devuelve los bloques que Postgres lee del disco
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM public.estudiantes3
WHERE indice = 500;

---------- Cuestión 12

-- Borrar las tablas anteriores
DROP TABLE IF EXISTS public.estudiantes CASCADE;
DROP TABLE IF EXISTS public.estudiantes2 CASCADE;
DROP TABLE IF EXISTS public.estudiantes3 CASCADE;

-- Creo una tabla estudiantes2
CREATE TABLE IF NOT EXISTS public.estudiantes2 (
    estudiante_id SERIAL PRIMARY KEY,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT
);

-- Insertamos en la tabla los datos creados con el archivo estudiantes.py
COPY public.estudiantes2(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

DROP TABLE IF EXISTS public.estudiantes2_temp;

-- Reescribir la tabla ordenando por índice
CREATE TABLE public.estudiantes2_temp AS
SELECT *
FROM public.estudiantes2
ORDER BY indice, estudiante_id;

-- Vacio la tabla original y vuelvo a insertar en orden
TRUNCATE TABLE public.estudiantes2;

INSERT INTO public.estudiantes2(estudiante_id, nombre, codigo_carrera, edad, indice)
SELECT estudiante_id, nombre, codigo_carrera, edad, indice
FROM public.estudiantes2_temp
ORDER BY indice, estudiante_id;

-- Limpiar el temporal
DROP TABLE public.estudiantes2_temp;

-- Actualiza las estadísticas
ANALYZE public.estudiantes2;

-- Comprobar los bloques
SELECT pg_relation_size('public.estudiantes2') / 8192 AS bloques_estudiantes2;

---------- Cuestión 13

-- Crear índice B-Tree
CREATE INDEX IF NOT EXISTS idx_estudiantes2_estudiante_id
ON public.estudiantes2 USING btree (estudiante_id);

-- ¿Dónde se almacena físicamente?

    -- Ruta física
SELECT pg_relation_filepath('public.idx_estudiantes2_estudiante_id') AS fichero_relativo;

    -- Dónde está en la tabla
SELECT c.relname AS indice, COALESCE(t.spcname, 'pg_default') AS tablespace
FROM pg_class c
LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
WHERE c.relname = 'idx_estudiantes2_estudiante_id';

-- Compruebo el tamaño del bloque
SHOW block_size;

-- Tamaño en bytes y en bloques
SELECT
    pg_relation_size('public.idx_estudiantes2_estudiante_id') AS bytes_indice,
    pg_relation_size('public.idx_estudiantes2_estudiante_id') / 8192 AS bloques_indice;

-- Niveles del B-Tree
CREATE EXTENSION IF NOT EXISTS pageinspect;

SELECT level + 1 AS num_niveles
FROM bt_metap('public.idx_estudiantes2_estudiante_id');

-- Cuantos bloques y tuplas tiene por nivel
SELECT
  s.btpo_level AS nivel,
  COUNT(*) AS bloques_en_nivel,
  ROUND(AVG(s.live_items), 2) AS tuplas_media_por_bloque
FROM generate_series(
  1::bigint,
  (SELECT relpages::bigint - 1
   FROM pg_class
   WHERE relname = 'idx_estudiantes2_estudiante_id')
) AS blkno
CROSS JOIN LATERAL bt_page_stats('public.idx_estudiantes2_estudiante_id', blkno) AS s
GROUP BY s.btpo_level
ORDER BY s.btpo_level DESC;

---------- Cuestión 14

-- Verificar Lk
SELECT pg_column_size(estudiante_id)
FROM estudiantes2
LIMIT 1;

-- Tamaño bloque
SHOW block_size;

---------- Cuestión 15

-- Crear índice hash
CREATE INDEX IF NOT EXISTS idx_estudiantes2_estudiante_id_hash
ON public.estudiantes2 USING hash (estudiante_id);

-- ¿Dónde se almacena físicamente?

    -- Ruta física
SELECT pg_relation_filepath('public.idx_estudiantes2_estudiante_id_hash') AS fichero_relativo;

    -- Dónde está en la tabla
SELECT c.relname AS indice, COALESCE(t.spcname, 'pg_default') AS tablespace
FROM pg_class c
LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
WHERE c.relname = 'idx_estudiantes2_estudiante_id_hash';

-- Compruebo el tamaño del bloque
SHOW block_size;

-- Tamaño en bytes y en bloques
SELECT
    pg_relation_size('public.idx_estudiantes2_estudiante_id_hash') AS bytes_indice,
    pg_relation_size('public.idx_estudiantes2_estudiante_id_hash') / 8192 AS bloques_indice;

-- Cajones del Hash
SELECT *
FROM hash_metapage_info(get_raw_page('public.idx_estudiantes2_estudiante_id_hash', 0));

-- Número de tuplas totales
SELECT COUNT(*) AS tuplas_totales
FROM public.estudiantes2;

-- Tuplas de media en un cajón
WITH meta AS (
  SELECT *
  FROM hash_metapage_info(get_raw_page('public.idx_estudiantes2_estudiante_id_hash', 0))
),
t AS (
  SELECT COUNT(*)::numeric AS n FROM public.estudiantes2
)
SELECT
  (meta.maxbucket + 1) AS num_cajones,
  ROUND(t.n / (meta.maxbucket + 1), 2) AS tuplas_media_por_cajon
FROM meta, t;

---------- Cuestión 16

-- Ver cajones
SELECT *
FROM hash_metapage_info(get_raw_page('public.idx_estudiantes2_estudiante_id_hash', 0));

---------- Cuestión 17

-- Crear índice B-Tree
CREATE INDEX IF NOT EXISTS idx_estudiantes2_indice_btree
ON public.estudiantes2 USING btree (indice);

-- ¿Dónde se almacena físicamente?

    -- Ruta física
SELECT pg_relation_filepath('public.idx_estudiantes2_indice_btree') AS fichero_relativo;

    -- Dónde está en la tabla
SELECT c.relname AS indice, COALESCE(t.spcname, 'pg_default') AS tablespace
FROM pg_class c
LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
WHERE c.relname = 'idx_estudiantes2_indice_btree';

-- Compruebo el tamaño del bloque
SHOW block_size;

-- Tamaño en bytes y en bloques
SELECT
    pg_relation_size('public.idx_estudiantes2_indice_btree') AS bytes_indice,
    pg_relation_size('public.idx_estudiantes2_indice_btree') / 8192 AS bloques_indice;

-- Niveles del B-Tree
SELECT level + 1 AS num_niveles
FROM bt_metap('public.idx_estudiantes2_indice_btree');

-- Cuantos bloques y tuplas tiene por nivel
SELECT
  s.btpo_level AS nivel,
  COUNT(*) AS bloques_en_nivel,
  ROUND(AVG(s.live_items), 2) AS tuplas_media_por_bloque
FROM generate_series(
  1::bigint,
  (SELECT relpages::bigint - 1
   FROM pg_class
   WHERE relname = 'idx_estudiantes2_indice_btree')
) AS blkno
CROSS JOIN LATERAL bt_page_stats('public.idx_estudiantes2_indice_btree', blkno) AS s
GROUP BY s.btpo_level
ORDER BY s.btpo_level DESC;

---------- Cuestión 18

-- Verificar Lk
SELECT pg_column_size(indice)
FROM estudiantes2
LIMIT 1;

---------- Cuestión 19

-- Crear índice hash
CREATE INDEX IF NOT EXISTS idx_estudiantes2_indice_hash
ON public.estudiantes2 USING hash (indice);

-- ¿Dónde se almacena físicamente?

    -- Ruta física
SELECT pg_relation_filepath('public.idx_estudiantes2_indice_hash') AS fichero_relativo;

    -- Dónde está en la tabla
SELECT c.relname AS indice, COALESCE(t.spcname, 'pg_default') AS tablespace
FROM pg_class c
LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
WHERE c.relname = 'idx_estudiantes2_indice_hash';

-- Compruebo el tamaño del bloque
SHOW block_size;

-- Tamaño en bytes y en bloques
SELECT
    pg_relation_size('public.idx_estudiantes2_indice_hash') AS bytes_indice,
    pg_relation_size('public.idx_estudiantes2_indice_hash') / 8192 AS bloques_indice;

-- Cajones del Hash
SELECT *
FROM hash_metapage_info(get_raw_page('public.idx_estudiantes2_indice_hash', 0));

-- Número de tuplas totales
SELECT COUNT(*) AS tuplas_totales
FROM public.estudiantes2;

-- Tuplas de media en un cajón
WITH meta AS (
  SELECT *
  FROM hash_metapage_info(get_raw_page('public.idx_estudiantes2_indice_hash', 0))
),
t AS (
  SELECT COUNT(*)::numeric AS n FROM public.estudiantes2
)
SELECT
  (meta.maxbucket + 1) AS num_cajones,
  ROUND(t.n / (meta.maxbucket + 1), 2) AS tuplas_media_por_cajon
FROM meta, t;

---------- Cuestión 20

-- Ver cajones
SELECT *
FROM hash_metapage_info(get_raw_page('public.idx_estudiantes2_indice_hash', 0));

---------- Cuestión 21



---------- Cuestión 22

-- Borrar las posibles tablas existentes
DROP TABLE IF EXISTS public.estudiantes CASCADE;
DROP TABLE IF EXISTS public.estudiantes2 CASCADE;
DROP TABLE IF EXISTS public.estudiantes2_temp CASCADE;
DROP TABLE IF EXISTS public.estudiantes3 CASCADE;

-- Creamos la tabla estudiantes e insertamos los datos en ella
CREATE TABLE IF NOT EXISTS public.estudiantes (
    estudiante_id SERIAL PRIMARY KEY ,
    nombre TEXT,
    codigo_carrera INT,
    edad INT,
    indice INT
);

COPY public.estudiantes(nombre, codigo_carrera, edad, indice)
FROM 'C:\estudiantes.csv'
WITH (FORMAT csv, HEADER true);

-- Actualizar estadísticas
ANALYZE public.estudiantes;

-- Crear índice primario tipo árbol sobre el campo índice
CREATE INDEX IF NOT EXISTS idx_estudiantes_indice_btree ON public.estudiantes USING btree (indice);

-- Crear índice primario tipo árbol sobre el campo codigo_carrera
CREATE INDEX IF NOT EXISTS idx_estudiantes_codigo_carrera_btree ON public.estudiantes USING btree (codigo_carrera);

-- Crear índice tipo hash sobre el campo estudiante_id
CREATE INDEX IF NOT EXISTS idx_estudiantes_estudiante_id_hash ON public.estudiantes USING hash (estudiante_id);

-- Crear índice tipo hash sobre el campo indice
CREATE INDEX IF NOT EXISTS idx_estudiantes_indice_hash ON public.estudiantes USING hash (indice);

---------- Cuestión 23

----- Apartado 1

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

-- Ejecutar la consulta, contar tuplas
SELECT COUNT(*) AS tuplas
FROM public.estudiantes
WHERE codigo_carrera = 50;

SELECT *
FROM public.estudiantes
WHERE codigo_carrera = 50;

-- Medir bloques leídos

    -- De la tabla
SELECT
  heap_blks_read,
  heap_blks_hit,
  idx_blks_read,
  idx_blks_hit
FROM pg_statio_user_tables
WHERE relname = 'estudiantes';

    -- De los índices
SELECT
  indexrelname,
  idx_blks_read,
  idx_blks_hit
FROM pg_statio_user_indexes
WHERE relname = 'estudiantes'
ORDER BY indexrelname;

----- Apartado 2

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

-- Ejecutar la consulta, contar tuplas
SELECT COUNT(*) AS tuplas
FROM public.estudiantes
WHERE estudiante_id = 80000;

SELECT *
FROM public.estudiantes
WHERE estudiante_id = 80000;

-- Medir bloques leídos

    -- De la tabla
SELECT
  heap_blks_read,
  heap_blks_hit,
  idx_blks_read,
  idx_blks_hit
FROM pg_statio_user_tables
WHERE relname = 'estudiantes';

    -- De los índices
SELECT
  indexrelname,
  idx_blks_read,
  idx_blks_hit
FROM pg_statio_user_indexes
WHERE relname = 'estudiantes'
ORDER BY indexrelname;

----- Apartado 3

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

----- Apartado 4

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

----- Apartado 5

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

----- Apartado 6

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

----- Apartado 7

-- Reinicializamos estadísticas
SELECT pg_stat_reset();

---------- Cuestión 24



---------- Cuestión 25



---------- Cuestión 26