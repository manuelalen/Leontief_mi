CREATE PROCEDURE dev.sp_merge_to_target()
BEGIN
  DECLARE done INT DEFAULT 0;

  DECLARE v_source_db VARCHAR(64);
  DECLARE v_source_table VARCHAR(64);
  DECLARE v_target_db VARCHAR(64);
  DECLARE v_target_table VARCHAR(64);

  DECLARE cur CURSOR FOR
    SELECT
      JSON_UNQUOTE(JSON_EXTRACT(source, '$.db'))    AS source_db,
      JSON_UNQUOTE(JSON_EXTRACT(source, '$.table')) AS source_table,
      JSON_UNQUOTE(JSON_EXTRACT(target, '$.db'))    AS target_db,
      JSON_UNQUOTE(JSON_EXTRACT(target, '$.table')) AS target_table
    FROM dev.t_metadata
    WHERE active = TRUE AND INGESTION_NAME = '1.MASS-INGESTION'
    ORDER BY id;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  -- Coge el primer target activo (asumimos que todos apuntan al mismo target)
  SELECT
    JSON_UNQUOTE(JSON_EXTRACT(target, '$.db')),
    JSON_UNQUOTE(JSON_EXTRACT(target, '$.table'))
  INTO v_target_db, v_target_table
  FROM dev.t_metadata
  WHERE active = TRUE AND INGESTION_NAME = '1.MASS-INGESTION'
  ORDER BY id
  LIMIT 1;

  IF v_target_db IS NULL OR v_target_table IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No hay filas ACTIVE=TRUE en dev.t_metadata (no hay target).';
  END IF;

  -- Crear tabla target si no existe
  SET @sql_create = CONCAT(
    'CREATE TABLE IF NOT EXISTS `', v_target_db, '`.`', v_target_table, '` (',
    '  cod_producto VARCHAR(64) NOT NULL,',
    '  `id` INT NOT NULL,',
    '  valor DOUBLE NOT NULL,',
    '  fecha TIMESTAMP NULL,',
    '  PRIMARY KEY (cod_producto, `id`)',
    ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'
  );
  PREPARE stmt FROM @sql_create;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  -- Recorre las fuentes y hace UPSERT
  OPEN cur;

  read_loop: LOOP
    FETCH cur INTO v_source_db, v_source_table, v_target_db, v_target_table;
    IF done = 1 THEN
      LEAVE read_loop;
    END IF;

    -- UPSERT (MERGE) desde la tabla fuente hacia el target
    -- Mapeo: i -> id, x -> valor, created_at -> fecha
    SET @sql_merge = CONCAT(
      'INSERT INTO `', v_target_db, '`.`', v_target_table, '` (cod_producto, `id`, valor, fecha) ',
      'SELECT ''', v_source_table, ''' AS cod_producto, s.i AS `id`, s.x AS valor, s.created_at AS fecha ',
      'FROM `', v_source_db, '`.`', v_source_table, '` s ',
      'ON DUPLICATE KEY UPDATE ',
      '  valor = VALUES(valor), ',
      '  fecha = VALUES(fecha);'
    );

    PREPARE stmt2 FROM @sql_merge;
    EXECUTE stmt2;
    DEALLOCATE PREPARE stmt2;

  END LOOP;

  CLOSE cur;

END$$

DELIMITER ;
