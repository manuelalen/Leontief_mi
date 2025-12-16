CREATE VIEW dev.f_cdp
as
SELECT 
c.cod_producto,
m.nombre,
c.id,
c.valor
FROM dev.cdp c
LEFT JOIN dev.m_productos m
ON c.cod_producto = LOWER(m.id);

-- lambda
CREATE VIEW dev.f_cdp_tiempo_prod
as
SELECT
  c.cod_producto,
  SUM(c.valor * t.tiempo_produccion) AS lambda_horas_persona
FROM dev.cdp c
JOIN dev.m_tiempos_produccion t
  ON t.cod_producto = c.cod_producto
 AND t.id = c.id
GROUP BY c.cod_producto;
