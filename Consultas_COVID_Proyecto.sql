SELECT location, fecha, total_cases, new_cases, total_deaths, population
FROM covid_muertes
where continent is not NULL
order by 1,2
-- Línea para guardar el resultado de una consulta en forma de archivo texto o csv
INTO outfile 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\consulta.txt';


-- A. Total cases vs population
SELECT location, año_mes, avg(RatioContagio) FROM (   
SELECT location, 
	fecha,
    concat(extract(YEAR FROM fecha), "-", extract(MONTH FROM fecha) ) as año_mes ,
    total_cases, 
    population, 
    (total_cases / population) * 100 as RatioContagio
FROM covid_muertes) A
group by location, año_mes;


-- B. PAISES CON MAYOR TASA DE CONTAGIO
SELECT location, population, MAX(total_cases) , (max(total_cases) / population) * 100 as RatioContagio
FROM covid_muertes
where continent <> "" 	-- Eliminar los agregados como ASIA, AFRICA, etc
group by location, population
order by RatioContagio DESC;

-- C. PAISES CON MAYOR % DE MUERTOS POR COVID (muertos / contagiados)
SELECT location, max(total_deaths) as total_muertos, max(total_deaths) / max(total_cases) * 100 as tasa_muertos FROM covid_muertes
where continent <> ""
group by location
order by tasa_muertos DESC;

-- D. PAISES CON MAYOR NUMERO DE MUERTOS
SELECT location, MAX(total_deaths) as total_muertos FROM covid_muertes
WHERE continent <> ""
GROUP BY location
ORDER BY total_muertos DESC;


-- E. Contagios por país, y meses
WITH contagios_mensual as 
		(SELECT location,  year(fecha) as año, month(fecha) as mes, max(total_cases) as contagios_mes
		FROM covid_muertes
		WHERE continent <> ""
		GROUP BY location, año, mes)
SELECT location, año, mes, contagios_mes, contagios_mes_pasado, CASE 
WHEN contagios_mes_pasado is null THEN contagios_mes
ELSE contagios_mes - contagios_mes_pasado
END AS crecimiento
	FROM (
	SELECT location, año, mes, contagios_mes, 
	lag(contagios_mes, 1) over(partition by location order by location, año, mes) as contagios_mes_pasado
FROM contagios_mensual) temp;


-- ANALISIS POR CONTINENTE
-- A. Continent con mayor numero de muertos y contagiados
SELECT continent, sum(casos) as total_casos, sum(muertos) AS total_muertos FROM 
	(SELECT continent, location,  max(total_cases) as casos, max(total_deaths) as muertos FROM covid_muertes
	WHERE continent <> ""
	group by continent, location ) A
GROUP BY  continent
ORDER BY total_casos DESC;


-- ANALISIS GLOBAL
-- A. Variacion Porcentual de muertes desde Enero - 2020.
WITH global_muertes AS 
		(SELECT YEAR(fecha) AS año, MONTH(fecha) AS mes, sum(new_deaths) AS total_muertes FROM covid_muertes
		WHERE continent <> ""
		GROUP BY año, mes)
SELECT  año, mes, ((total_muertes - muertes_mes_pasado) / muertes_mes_pasado * 100) as var_porc_muertes  
FROM (
	SELECT año, mes, total_muertes, LAG(total_muertes,1) OVER (ORDER BY año, mes) AS muertes_mes_pasado 
	FROM global_muertes) temp;


-- B. Porcentaje de muertos por covid al día
SELECT fecha, sum(new_cases) as total_casos, 
	sum(new_deaths) as total_muertos,
    (sum(new_deaths) / sum(new_cases) *100)  as PorcentajeMuertos
FROM covid_muertes
WHERE continent <> "" 
GROUP BY fecha
order by fecha;


-- INFORMACION SOBRE LAS VACUNAS
select * from covid_vacunas;

-- Cantidad testeos por pais
SELECT location, max(total_tests) as n_testeos FROM covid_vacunas
WHERE continent <> "" 
GROUP BY location;

-- Personas vacunadas vs población por país
SELECT vac.location, p.poblacion,  
max(people_vaccinated) as personas_vacunadas
FROM covid_vacunas vac
JOIN paises p ON vac.iso_code = p.codigo_pais
WHERE vac.continent <> ""
GROUP BY vac.location 
ORDER BY vac.location; 


-- % de la poblacion mundial vacunada
WITH poblacion_vacunada as (
		SELECT v.location, poblacion, max(people_vaccinated) as personas_vacunadas
		FROM covid_vacunas v INNER JOIN paises p ON p.codigo_pais = v.iso_code
		WHERE v.continent <> "" GROUP BY v.location )
SELECT SUM(poblacion) AS poblacion_mundial,
SUM(personas_vacunadas) AS poblacion_vacunada,
SUM(personas_vacunadas) / SUM(poblacion) *100 AS PorcAvance 
FROM poblacion_vacunada;


-- AVANCE DE VACUNACION AL DIA POR PAISES
	-- Crear una tabla resumen donde se mostrará los resultados
DROP TABLE IF EXISTS resumen_avance_vacunacion;
CREATE TABLE resumen_avance_vacunacion 
(
	location varchar(100),
	fecha date,
	poblacion bigint,
	vacunas_dia bigint,
	vacunas_acum bigint,
	avance_vacunacion decimal(8,3));
INSERT INTO resumen_avance_vacunacion
SELECT location, fecha, poblacion, vacunas_dia, vacunas_acum, vacunas_acum / poblacion * 100 as porc_vacunacion
	FROM (
		SELECT v.location, fecha, poblacion, 
		sum(new_vaccinations) as vacunas_dia,
		sum(new_vaccinations) OVER (PARTITION BY location ORDER BY location, fecha) vacunas_acum
		FROM covid_vacunas v JOIN paises p ON p.codigo_pais = v.iso_code 
		WHERE v.continent <> "" AND year(fecha) > 2020 
		GROUP BY location, fecha, poblacion
		ORDER BY location, fecha) temp;
select * from resumen_avance_vacunacion
WHERE location like '%per%';
