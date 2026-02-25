# ============================================================================
# EJERCICIO 5 - ENOE NUEVA (2022-2025) - VERSIÓN OPTIMIZADA PARA MEMORIA
# Análisis de microdatos de mercado laboral en México
# ============================================================================
# Fecha: 14 de febrero de 2026
#
# DESCRIPCIÓN GENERAL:
#   Este script procesa los microdatos de la Encuesta Nacional de Ocupación
#   y Empleo (ENOE Nueva) para los años 2022-2025 (15 trimestres). Calcula
#   indicadores trimestrales del mercado laboral mexicano requeridos para el
#   Ejercicio 5:
#     5b) Tasa de desempleo
#     5c) Tasa de subempleo (subocupación)
#     5d) Trabajadores fuera de la FLab disponibles para trabajar
#     5e) Distribución del tamaño de empresa
#     5f) Proporción de ocupados buscando otro empleo
#     5g) Ingreso promedio por grupo de edad
#     5h) Distribución de ocupados por sexo
#     5i) Tasas de transición empleo <-> desempleo (panel rotativo)
#
# ESTRATEGIA DE OPTIMIZACIÓN DE MEMORIA:
#   - Solo se cargan las columnas necesarias de cada archivo CSV
#   - Se libera memoria (rm() + gc()) después de cada paso
#   - Se procesan los trimestres de forma secuencial (no simultánea)
#
# FUENTES DE DATOS:
#   INEGI - ENOE Nueva (a partir de 2020, diseño muestral renovado)
#   https://www.inegi.org.mx/programas/enoe/15ymas/
#   Cada trimestre contiene dos archivos CSV relevantes:
#     - SDEMT*.csv  : Sociodemográficos (características de la persona)
#     - COE1T*.csv  : Cuestionario de Ocupación y Empleo (parte 1)
#
# DEPENDENCIAS:
#   - R >= 4.0
#   - JuliaCall (para el inciso 5i, panel rotativo)
#   - Julia >= 1.6 con paquete DataFrames.jl instalado
#   - tidyverse, readr, kableExtra, stargazer
#
# CÓMO REPLICAR:
#   1. Descarga los datos trimestrales de ENOE Nueva desde INEGI
#   2. Descomprime cada trimestre en su carpeta correspondiente
#      (ej. "enoe_n_2022_trim1_csv") dentro de RUTA_BASE
#   3. Ajusta RUTA_BASE a la carpeta donde guardaste los datos
#   4. Ajusta las rutas de escritura en los write_csv() al final de cada sección
#   5. Ejecuta el script completo (estimado: 5-10 minutos)
# ============================================================================

library(JuliaCall)    # Puente R <-> Julia (necesario para el inciso 5i)
library(tidyverse)    # Manipulación de datos y visualización (dplyr, ggplot2, etc.)
library(readr)        # Lectura eficiente de CSV con control de tipos y encoding
library(kableExtra)   # Tablas formateadas (opcional, referenciado pero no activo)
library(stargazer)    # Tablas de estadísticas descriptivas (opcional, comentado)

# ============================================================================
# CONFIGURACIÓN - AJUSTA ESTAS RUTAS ANTES DE CORRER EL SCRIPT
# ============================================================================

# RUTA_BASE: carpeta raíz donde están todas las subcarpetas de trimestres.
# Cada subcarpeta debe contener los archivos CSV del trimestre (SDEMT, COE1T, etc.)
RUTA_BASE <- "C:/Users/spart/Desktop/Datos Tarea Macro 1"

# Vector con los nombres exactos de las carpetas para cada trimestre.
# Convención de nombres: enoe_n_{año}_trim{trimestre}_csv
# Ajusta si INEGI cambió el formato del nombre en alguna descarga.
carpetas_trimestres <- c(
  # 2022 (4 trimestres)
  "enoe_n_2022_trim1_csv",
  "enoe_n_2022_trim2_csv",
  "enoe_n_2022_trim3_csv",
  "enoe_n_2022_trim4_csv",
  # 2023 (4 trimestres)
  "enoe_n_2023_trim1_csv",
  "enoe_n_2023_trim2_csv",
  "enoe_n_2023_trim3_csv",
  "enoe_n_2023_trim4_csv",
  # 2024 (4 trimestres)
  "enoe_n_2024_trim1_csv",
  "enoe_n_2024_trim2_csv",
  "enoe_n_2024_trim3_csv",
  "enoe_n_2024_trim4_csv",
  # 2025 (3 trimestres disponibles al momento de correr)
  "enoe_n_2025_trim1_csv",
  "enoe_n_2025_trim2_csv",
  "enoe_n_2025_trim3_csv"
)

# Tabla de metadatos: mapea cada carpeta a su año y número de trimestre.
# Se construye automáticamente a partir del vector anterior.
info_trimestres <- tibble(
  carpeta   = carpetas_trimestres,
  anio      = c(rep(2022, 4), rep(2023, 4), rep(2024, 4), rep(2025, 3)),
  trimestre = c(1:4, 1:4, 1:4, 1:3)
)


# ============================================================================
# VARIABLES A CONSERVAR
# ============================================================================
# Solo cargamos las columnas que necesitamos, reduciendo uso de memoria.
# Fuente de definiciones: diccionario de datos ENOE Nueva (INEGI).
#
# Grupos de variables:
#
# CLAVE PRIMARIA (panel / merge entre SDEMT y COE1T):
#   cd_a      : código de área
#   ent       : entidad federativa
#   upm       : unidad primaria de muestreo
#   con       : consecutivo del hogar en la UPM
#   d_sem     : semana de levantamiento
#   n_pro_viv : número progresivo de la vivienda
#   v_sel     : vivienda seleccionada
#   n_hog     : número de hogar en la vivienda
#   h_mud     : indicador de mudanza del hogar
#   n_ren     : número de renglón (persona en el hogar)
#   tipo      : tipo de entrevista
#   mes_cal   : mes calendario del levantamiento
#
# SOCIODEMOGRÁFICAS:
#   eda       : edad en años cumplidos
#   sex       : sexo (1=Hombre, 2=Mujer)
#
# LABORALES:
#   clase2    : clasificación laboral
#                 1=Ocupados, 2=Desocupados, 3=PNEA disponible, 4=PNEA no disponible
#   sub_o     : subocupación (1=Sí, 0=No), solo para ocupados
#   dispo     : disponibilidad para trabajar (PNEA)
#   fac_tri   : factor de expansión trimestral (ponderador muestral)
#               Indica a cuántas personas de la población representa cada registro
#   ingocup   : ingreso mensual de la ocupación principal (pesos corrientes)
#   busqueda  : búsqueda de otro empleo (1=Sí) para ocupados
#
# UNIDAD ECONÓMICA:
#   ambito2   : ámbito de la unidad económica (tamaño/sector del establecimiento)
#                 2,3=Micronegocios sin y con local, 4=Pequeños, 5=Medianos,
#                 6=Grandes, 7=Gobierno, 8=Otros
#   p4a       : rama de actividad económica
#
# METADATA (usados solo para filtrar, luego se eliminan):
#   r_def     : resultado de la entrevista (0/00=entrevista completa)
#   c_res     : condición de residencia (1=residente habitual, 3=residente de paso)

vars_clave <- c(
  "cd_a", "ent", "upm", "con", "d_sem", "n_pro_viv",
  "v_sel", "n_hog", "h_mud", "n_ren", "tipo", "mes_cal",
  "eda", "sex",
  "clase2", "sub_o", "dispo", "fac_tri", "ingocup", "busqueda",
  "ambito2", "p4a",
  "r_def", "c_res"
)


# ============================================================================
# FUNCIÓN: NORMALIZAR NOMBRES DE COLUMNAS
# ============================================================================
# Propósito: Eliminar el prefijo "cve_" que INEGI añade en algunos trimestres
#            a ciertos nombres de columnas (ej. "cve_ent" -> "ent").
#            Esto asegura consistencia entre trimestres.
# Entrada:  data frame con nombres de columnas posiblemente inconsistentes
# Salida:   el mismo data frame con nombres normalizados (sin prefijo "cve_")

normalizar_columnas <- function(df) {
  names(df) <- str_replace(names(df), "^cve_", "")
  return(df)
}

# ============================================================================
# FUNCIÓN PRINCIPAL: PROCESAR UN TRIMESTRE
# ============================================================================
# Propósito: Lee, filtra, combina y estandariza los datos de un trimestre.
#
# Pasos internos:
#   1. Detecta los archivos SDEMT y COE1T en la carpeta del trimestre
#   2. Lee SOLO las columnas necesarias (optimización de memoria)
#   3. Filtra la población válida (entrevistas completas, residentes, edad >= 15)
#   4. Hace un left join entre SDEMT y COE1T por la clave primaria
#   5. Convierte tipos de variables y agrega año/trimestre/período como columnas
#
# Parámetros:
#   ruta_carpeta : ruta completa a la carpeta del trimestre (string)
#   anio         : año del trimestre (entero, ej. 2022)
#   trimestre    : número de trimestre (entero, 1-4)
#
# Retorna: tibble con los datos del trimestre, o NULL si hay error

procesar_trimestre_enoen_optimizado <- function(ruta_carpeta, anio, trimestre) {
  
  cat("\n========================================\n")
  cat(sprintf("Procesando %d Trimestre %d\n", anio, trimestre))
  cat("========================================\n")
  
  # --- PASO 1: Detectar archivos ---
  # Busca todos los CSV en la carpeta y extrae los que tienen SDEMT o COE1T en el nombre
  archivos <- list.files(ruta_carpeta, pattern = "\\.csv$", full.names = TRUE)
  
  sdemt_file <- str_subset(archivos, "SDEMT")
  coe1t_file <- str_subset(archivos, "COE1T")
  
  # Si falta alguno de los dos archivos, se omite este trimestre con advertencia
  if (length(sdemt_file) == 0 || length(coe1t_file) == 0) {
    warning(sprintf("Archivos faltantes en %s. Saltando...", ruta_carpeta))
    return(NULL)
  }
  
  cat(sprintf("  SDEMT: %s\n", basename(sdemt_file)))
  cat(sprintf("  COE1T: %s\n", basename(coe1t_file)))
  
  # --- PASO 2: Cargar SOLO las columnas necesarias ---
  # Primero leemos 0 filas para obtener los nombres de columna sin cargar datos
  cat("  Cargando columnas necesarias...\n")
  
  sdemt_cols_all <- names(read_csv(sdemt_file, n_max = 0, col_types = cols(.default = "c")))
  coe1t_cols_all <- names(read_csv(coe1t_file, n_max = 0, col_types = cols(.default = "c")))
  
  # Normalizar: quitar prefijos "cve_" y convertir a minúsculas para comparar
  sdemt_cols_norm <- str_replace(tolower(sdemt_cols_all), "^cve_", "")
  coe1t_cols_norm <- str_replace(tolower(coe1t_cols_all), "^cve_", "")
  
  # Intersección: solo seleccionamos las columnas que están en vars_clave Y en el archivo
  vars_sdemt <- intersect(vars_clave, sdemt_cols_norm)
  vars_coe1t <- intersect(vars_clave, coe1t_cols_norm)
  
  # Re-mapear a los nombres originales (antes de normalizar) para hacer la selección
  sdemt_cols_select <- sdemt_cols_all[match(vars_sdemt, sdemt_cols_norm)]
  coe1t_cols_select <- coe1t_cols_all[match(vars_coe1t, coe1t_cols_norm)]
  
  # Cargar los CSVs con encoding latin1 (característico de archivos INEGI)
  # Todas las columnas se leen como texto ("c") para evitar conversiones automáticas
  sdemt_raw <- read_csv(
    sdemt_file,
    col_select = all_of(sdemt_cols_select),
    col_types  = cols(.default = "c"),
    locale     = locale(encoding = "latin1")
  )
  
  coe1t_raw <- read_csv(
    coe1t_file,
    col_select = all_of(coe1t_cols_select),
    col_types  = cols(.default = "c"),
    locale     = locale(encoding = "latin1")
  )
  
  # Normalizar nombres de columnas de los datos ya cargados
  names(sdemt_raw) <- str_replace(tolower(names(sdemt_raw)), "^cve_", "")
  names(coe1t_raw) <- str_replace(tolower(names(coe1t_raw)), "^cve_", "")
  
  cat(sprintf("  SDEMT: %d filas, %d columnas (reducido desde %d)\n", 
              nrow(sdemt_raw), ncol(sdemt_raw), length(sdemt_cols_all)))
  cat(sprintf("  COE1T: %d filas, %d columnas (reducido desde %d)\n", 
              nrow(coe1t_raw), ncol(coe1t_raw), length(coe1t_cols_all)))
  
  # --- PASO 3: Filtrar población válida en SDEMT ---
  cat("  Filtrando población válida...\n")
  
  # Criterios de inclusión:
  #   r_def %in% c("0","00") : entrevista completa (no ausentes, no rechazo)
  #   c_res %in% c("1","3")  : residente habitual o residente de paso
  #   eda >= 15              : personas en edad de trabajar (15+ años)
  sdemt_clean <- sdemt_raw %>%
    filter(
      r_def %in% c("0", "00"),
      c_res %in% c("1", "3"),
      as.numeric(eda) >= 15
    ) %>%
    select(-r_def, -c_res)   # Eliminamos variables de filtro; ya no se necesitan
  
  rm(sdemt_raw)       # Liberar memoria del dataframe crudo
  gc(verbose = FALSE) # Forzar recolección de basura
  
  cat(sprintf("  Población válida: %d personas\n", nrow(sdemt_clean)))
  
  if (nrow(sdemt_clean) == 0) {
    warning("  No hay población válida")
    return(NULL)
  }
  
  # --- PASO 4: Merge entre SDEMT y COE1T ---
  # La clave primaria identifica unívocamente a cada persona en la encuesta.
  # Es la misma en ambos archivos, lo que permite cruzar información sociodemo-
  # gráfica (SDEMT) con información laboral detallada (COE1T).
  key_cols_base <- c(
    "cd_a", "ent", "upm", "con", "d_sem", "n_pro_viv",
    "v_sel", "n_hog", "h_mud", "n_ren", "tipo", "mes_cal"
  )
  
  # Solo usamos las llaves que efectivamente existen en ambos archivos
  key_cols <- intersect(key_cols_base, intersect(names(sdemt_clean), names(coe1t_raw)))
  
  # Preparar COE1T: mantener llave + variables restantes
  coe1t_vars  <- setdiff(names(coe1t_raw), key_cols)
  coe1t_clean <- coe1t_raw %>%
    select(all_of(c(key_cols, coe1t_vars)))
  
  rm(coe1t_raw)
  gc(FALSE)
  
  # Eliminar duplicados exactos por llave en COE1T (algunos trimestres tienen duplicados).
  # Esto garantiza un join 1-a-1 sin multiplicar filas.
  coe1t_clean <- coe1t_clean %>%
    distinct(across(all_of(key_cols)), .keep_all = TRUE)
  
  cat("  Realizando merge...\n")
  
  # Left join: conservamos todas las personas de SDEMT aunque no tengan registro en COE1T
  # suffix = c("", "_coe1") evita colisiones de nombres (las duplicadas de COE1T quedan con sufijo)
  merged <- sdemt_clean %>%
    left_join(coe1t_clean, by = key_cols, suffix = c("", "_coe1"))
  
  rm(sdemt_clean, coe1t_clean)
  gc(FALSE)
  
  # --- PASO 5: Conversión de tipos y metadata ---
  cat("  Convirtiendo tipos de variables...\n")
  
  # Pasamos de character a los tipos apropiados para cada variable.
  # Las variables "nuevas" (busqueda, ambito2, p4a) pueden no existir en todos los
  # trimestres; en ese caso el if() devuelve NA_character_ para esa columna.
  merged <- merged %>%
    mutate(
      # Numéricas
      eda     = as.numeric(eda),
      fac_tri = as.numeric(fac_tri),
      ingocup = as.numeric(ingocup),
      
      # Categóricas (se mantienen como texto para comparaciones exactas)
      clase2 = as.character(clase2),
      sub_o  = as.character(sub_o),
      dispo  = as.character(dispo),
      sex    = as.character(sex),
      
      # Variables que pueden o no estar en SDEMT según el trimestre
      busqueda = if ("busqueda" %in% names(.)) as.character(busqueda) else NA_character_,
      ambito2  = if ("ambito2"  %in% names(.)) as.character(ambito2)  else NA_character_,
      
      # Variable que viene de COE1T (puede no existir en todos los trimestres)
      p4a = if ("p4a" %in% names(.)) as.character(p4a) else NA_character_,
      
      # Identificadores de tiempo (se agregan como columnas para facilitar el análisis)
      anio      = anio,
      trimestre = trimestre,
      periodo   = sprintf("%dQ%d", anio, trimestre)   # Formato: "2022Q1"
    ) %>%
    select(-any_of(c("eda_coe1", "fac_tri_coe1")))  # Eliminar duplicados del join
  
  cat(sprintf("  Datos finales: %d filas, %d columnas\n", nrow(merged), ncol(merged)))
  cat(sprintf("  Memoria usada: %.1f MB\n", as.numeric(object.size(merged)) / 1024^2))
  
  return(merged)
}

# ============================================================================
# EJECUTAR: PROCESAR TODOS LOS TRIMESTRES EN UN LOOP
# ============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════╗\n")
cat("║         PROCESANDO ENOE NUEVA 2022-2025 (OPTIMIZADO)             ║\n")
cat("╚═══════════════════════════════════════════════════════════════════╝\n")

# Lista donde se irán acumulando los tibbles de cada trimestre
lista_trimestres <- list()

for (i in seq_len(nrow(info_trimestres))) {
  carpeta   <- info_trimestres$carpeta[i]
  anio      <- info_trimestres$anio[i]
  trimestre <- info_trimestres$trimestre[i]
  
  ruta_completa <- file.path(RUTA_BASE, carpeta)
  
  # Verificar que la carpeta existe antes de intentar procesar
  if (!dir.exists(ruta_completa)) {
    warning(sprintf("Carpeta no encontrada: %s. Saltando...", ruta_completa))
    next
  }
  
  # Llamar a la función de procesamiento
  datos_trim <- procesar_trimestre_enoen_optimizado(ruta_completa, anio, trimestre)
  
  # Solo agregar a la lista si se procesó exitosamente
  if (!is.null(datos_trim)) {
    lista_trimestres[[length(lista_trimestres) + 1]] <- datos_trim
  }
  
  # Liberar memoria cada 3 trimestres para evitar acumulación
  if (i %% 3 == 0) {
    cat("  → Liberando memoria...\n")
    gc(verbose = FALSE)
  }
}

# ============================================================================
# COMBINAR TODOS LOS TRIMESTRES EN UNA SOLA BASE
# ============================================================================

cat("\n========================================\n")
cat("Combinando todos los trimestres...\n")
cat("========================================\n")

# bind_rows() apila los tibbles de cada trimestre en una base longitudinal
enoe_completa <- bind_rows(lista_trimestres)

# Liberar la lista intermedia; ya no se necesita
rm(lista_trimestres)
gc(verbose = FALSE)

cat(sprintf("\n✓ Base de datos completa: %d personas, %d columnas\n", 
            nrow(enoe_completa), ncol(enoe_completa)))
cat(sprintf("  Memoria total usada: %.1f MB\n", 
            as.numeric(object.size(enoe_completa)) / 1024^2))

# ============================================================================
# EJERCICIO 5: CÁLCULOS DE INDICADORES
# ============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════╗\n")
cat("║                      EJERCICIO 5: RESULTADOS                       ║\n")
cat("╚═══════════════════════════════════════════════════════════════════╝\n")

# ============================================================================
# 5b) TASA DE DESEMPLEO TRIMESTRAL
# ============================================================================
# Definición:
#   tasa_desempleo = 100 * desocupados / PEA
#
# Donde:
#   PEA (Población Económicamente Activa) = ocupados + desocupados
#             = suma de fac_tri para clase2 ∈ {1, 2}
#   desocupados = suma de fac_tri para clase2 = 2
#
# Nota metodológica:
#   Se usa fac_tri (factor de expansión trimestral) como ponderador porque la
#   ENOE no es un censo sino una encuesta con diseño muestral complejo. Cada
#   registro representa a múltiples personas en la población. Sumar fac_tri
#   da el total poblacional estimado para cada categoría. Sin ponderar se
#   obtendría una "tasa de la muestra" que puede estar sesgada si el muestreo
#   no es uniforme entre estratos.

cat("\n5b) Tasa de desempleo trimestral\n")
cat("Calculada como: tasa = 100 * (desocupados / PEA)\n")

desempleo <- enoe_completa %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    pea          = sum(fac_tri[clase2 %in% c("1", "2")], na.rm = TRUE),
    desocupados  = sum(fac_tri[clase2 == "2"], na.rm = TRUE),
    tasa_desempleo = (desocupados / pea) * 100,
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(desempleo)
write_csv(desempleo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5b_desempleo.csv")

# ============================================================================
# 5c) TASA DE SUBEMPLEO (SUBOCUPACIÓN) TRIMESTRAL
# ============================================================================
# Definición:
#   tasa_subempleo = 100 * subocupados / ocupados
#
# Donde:
#   ocupados    = suma de fac_tri para clase2 = 1
#   subocupados = suma de fac_tri para clase2 = 1 AND sub_o = 1
#
# La subocupación identifica a ocupados que trabajan menos horas de las que
# desearían y están disponibles para trabajar más (definición INEGI).
# sub_o = 1 indica que el individuo es subocupado.
#
# Interpretación de sum(fac_tri[condición]):
#   Suma los ponderadores (fac_tri) de todas las personas que cumplen la
#   condición → total poblacional estimado de ese subgrupo.

cat("\n5c) Tasa de subempleo trimestral\n")
cat("Calculada como: tasa = 100 * (subocupados / ocupados)\n")

subempleo <- enoe_completa %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    ocupados     = sum(fac_tri[clase2 == "1"], na.rm = TRUE),
    subempleados = sum(fac_tri[clase2 == "1" & sub_o == "1"], na.rm = TRUE),
    tasa_subempleo = (subempleados / ocupados) * 100,
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(subempleo)
write_csv(subempleo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5c_subempleo.csv")

# Nota: el inciso 5d original (tasa de participación laboral) NO es lo que
# pide el ejercicio. El inciso 5d que sigue es "trabajadores fuera de la
# fuerza laboral disponibles para trabajar".
#
# El bloque comentado a continuación es la tasa de participación, guardado
# como referencia en caso de necesitarse en otro contexto.

# # --- 5d alternativo: Tasa de participación laboral (NO REQUERIDA) ---
# participacion <- enoe_completa %>%
#   group_by(anio, trimestre, periodo) %>%
#   summarise(
#     poblacion_15_mas = sum(fac_tri, na.rm = TRUE),
#     pea = sum(fac_tri[clase2 %in% c("1", "2")], na.rm = TRUE),
#     tasa_participacion = (pea / poblacion_15_mas) * 100,
#     .groups = "drop"
#   ) %>%
#   arrange(anio, trimestre)
# print(participacion)
# write_csv(participacion, "resultados_5d_participacion.csv")

# ============================================================================
# 5d) TRABAJADORES FUERA DE LA FUERZA LABORAL DISPONIBLES PARA TRABAJAR
# ============================================================================
# Definición:
#   tasa_disp = 100 * PNEA_disponible / PNEA_total
#
# Conceptos:
#   Fuerza laboral (FLab) = ocupados + desocupados (= PEA)
#   Fuera de la FLab = PNEA (Población No Económicamente Activa)
#   PNEA se divide en:
#     - PNEA disponible  (clase2 = 3): podrían trabajar pero no buscan activamente
#     - PNEA no disponible (clase2 = 4): no quieren o no pueden trabajar
#       (estudiantes, labores del hogar, jubilados, etc.)
#
# Interpretación: la "reserva laboral" como proporción de la PNEA total.
# A mayor tasa, mayor la fracción de inactivos que estarían disponibles
# si se les presentara una oportunidad laboral.

cat("\n5d) Trabajadores Fuera de la Fuerza Laboral Disponibles para Trabajar\n")
cat("Calculada como: tasa = 100 * (PNEA disponible / PNEA total)\n")

trabajadores_disponibles <- enoe_completa %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    pnea_disponible = sum(fac_tri[clase2 == "3"], na.rm = TRUE),
    pnea_total      = sum(fac_tri[clase2 %in% c("3", "4")], na.rm = TRUE),
    trab_disp = pnea_disponible,
    tasa_disp = if_else(pnea_total > 0, 100 * pnea_disponible / pnea_total, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(trabajadores_disponibles)
write_csv(trabajadores_disponibles, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5d_trabajadores_disponibles.csv")

# ============================================================================
# 5e) DISTRIBUCIÓN DEL TAMAÑO DE EMPRESA
# ============================================================================
# Definición: distribución porcentual de los ocupados (clase2=1) según el
# tamaño del establecimiento donde trabajan (variable ambito2).
#
# Codificación de ambito2 (INEGI):
#   2 = Micronegocios sin local
#   3 = Micronegocios con local
#   4 = Pequeños establecimientos (entre 6 y 15 trabajadores)
#   5 = Medianos establecimientos (entre 16 y 100 trabajadores)
#   6 = Grandes establecimientos (más de 100 trabajadores)
#   7 = Gobierno
#   8 = Otros (incluye hogares, agropecuario no especificado, etc.)
#
# Decisiones metodológicas:
#   - Se combinan 2 y 3 como "Micronegocios" (negocios familiares con/sin local)
#   - Se excluyen "Otros" y "no_especificado" del análisis final
#   - Se generan dos bases: una con todas las categorías y otra solo con
#     Micro/Pequeños/Medianos/Grandes (lo que pide el ejercicio)

cat("\n5e) Distribución del tamaño de empresa\n")

tamano_empresa <- enoe_completa %>%
  filter(clase2 == "1") %>%   # Solo ocupados
  mutate(
    tamano = case_when(
      ambito2 %in% c("2", "3") ~ "Micronegocios",
      ambito2 == "4"           ~ "Pequeños_establecimientos",
      ambito2 == "5"           ~ "Medianos_establecimientos",
      ambito2 == "6"           ~ "Grandes_establecimientos",
      ambito2 == "7"           ~ "Gobierno",
      ambito2 == "8"           ~ "Otros",
      TRUE                     ~ "no_especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, tamano) %>%
  summarise(ocupados = sum(fac_tri, na.rm = TRUE), .groups = "drop") %>%
  group_by(anio, trimestre, periodo) %>%
  mutate(porcentaje = (ocupados / sum(ocupados)) * 100) %>%
  # Ordenar categorías de forma lógica (de menor a mayor tamaño)
  mutate(tamano = factor(tamano, levels = c(
    "Micronegocios",
    "Pequeños_establecimientos",
    "Medianos_establecimientos",
    "Grandes_establecimientos",
    "Gobierno",
    "Otros",
    "no_especificado"
  ))) %>%
  arrange(anio, trimestre, tamano) %>%
  filter(!tamano %in% c("Otros", "no_especificado"))   # Excluir categorías residuales

print(tamano_empresa)
write_csv(tamano_empresa, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5e_tamano_empresa.csv")

# Sub-análisis: solo Micro, Pequeños, Medianos y Grandes (sin Gobierno)
# Recalcula el porcentaje sobre este subconjunto
empresas_pmg <- tamano_empresa %>%
  filter(tamano %in% c("Micronegocios", "Pequeños_establecimientos",
                       "Medianos_establecimientos", "Grandes_establecimientos")) %>%
  group_by(anio, trimestre, periodo) %>%
  mutate(porcentaje_pmg = 100 * ocupados / sum(ocupados, na.rm = TRUE)) %>%
  ungroup()

print(empresas_pmg)
write_csv(empresas_pmg, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5e_empresas_pmg.csv")

# ============================================================================
# 5f) PROPORCIÓN DE OCUPADOS QUE BUSCAN OTRO EMPLEO
# ============================================================================
# Definición:
#   proporcion_buscando = 100 * buscando_otro / ocupados
#
# Población base: ocupados (clase2 = 1)
# buscando_otro: ocupados con busqueda = 1
#
# Nota: Solo se toman en cuenta quienes respondieron (no NA en busqueda).
# El if_else() protege contra divisiones por cero si algún trimestre tuviera
# cero ocupados (situación teóricamente imposible pero se maneja por robustez).

cat("\n5f) Proporción de ocupados buscando otro empleo\n")
cat("Calculada como: tasa = 100 * (buscando_otro / ocupados)\n")

buscando_empleo <- enoe_completa %>%
  filter(clase2 == "1") %>%   # Solo ocupados
  group_by(anio, trimestre, periodo) %>%
  summarise(
    ocupados        = sum(fac_tri, na.rm = TRUE),
    buscando_otro   = sum(fac_tri[busqueda == "1"], na.rm = TRUE),
    proporcion_buscando = if_else(
      ocupados > 0,
      100 * buscando_otro / ocupados,
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(buscando_empleo)
write_csv(buscando_empleo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5f_buscando_empleo.csv")

# ============================================================================
# 5g) INGRESO PROMEDIO POR GRUPO DE EDAD
# ============================================================================
# Definición: promedio ponderado del ingreso mensual de la ocupación principal
#             (ingocup) por grupo de edad quinquenal, para ocupados.
#
# Variable: ingocup = ingreso mensual en pesos corrientes de la ocupación principal
#
# Por qué promedio ponderado (weighted.mean):
#   La ENOE es una encuesta, no un censo. Cada fila representa a múltiples
#   personas (fac_tri indica cuántas). El promedio simple promedia la muestra,
#   no la población. El promedio ponderado:
#     ȳ_w = Σ(fac_tri_i * ingocup_i) / Σ(fac_tri_i)
#   da el ingreso promedio de la población, corrigiendo la no-uniformidad
#   del diseño muestral.
#
# Grupos de edad: 15-24, 25-34, 35-44, 45-54, 55-64, 65+
#
# Se generan dos versiones:
#   - ingreso_edad_positivo : solo personas con ingocup > 0 (ingreso declarado)
#   - ingreso_edad_0        : todos los ocupados (incluyendo ingocup = 0),
#     calculando también la proporción con ingreso = 0 (trabajadores sin pago
#     declarado, ej. trabajadores familiares no remunerados)

cat("\n5g) Ingreso promedio por grupo de edad\n")

# Versión 1: Solo ocupados con ingreso positivo (excluye ingocup = 0 y NA)
ingreso_edad_positivo <- enoe_completa %>%
  filter(clase2 == "1", !is.na(ingocup), ingocup > 0) %>%
  mutate(
    grupo_edad = case_when(
      eda >= 15 & eda < 25 ~ "15-24",
      eda >= 25 & eda < 35 ~ "25-34",
      eda >= 35 & eda < 45 ~ "35-44",
      eda >= 45 & eda < 55 ~ "45-54",
      eda >= 55 & eda < 65 ~ "55-64",
      eda >= 65            ~ "65+",
      TRUE                 ~ "no_especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, grupo_edad) %>%
  summarise(
    n_personas      = sum(fac_tri, na.rm = TRUE),
    ingreso_promedio = weighted.mean(ingocup, fac_tri, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre, grupo_edad)

print(ingreso_edad_positivo)
write_csv(ingreso_edad_positivo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5g_ingreso_edad_positivo.csv")

# Versión 2: Todos los ocupados (incluye ingocup = 0), con estadísticas adicionales
# Esta versión documenta qué fracción de ocupados reporta ingreso cero,
# lo que es relevante para evaluar si el promedio condicional (versión 1) es representativo.
ingreso_edad_0 <- enoe_completa %>%
  filter(clase2 == "1", !is.na(ingocup)) %>%
  mutate(
    grupo_edad = case_when(
      eda >= 15 & eda < 25 ~ "15-24",
      eda >= 25 & eda < 35 ~ "25-34",
      eda >= 35 & eda < 45 ~ "35-44",
      eda >= 45 & eda < 55 ~ "45-54",
      eda >= 55 & eda < 65 ~ "55-64",
      eda >= 65            ~ "65+",
      TRUE                 ~ "no_especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, grupo_edad) %>%
  summarise(
    ocupados           = sum(fac_tri, na.rm = TRUE),
    
    # Proporción con ingreso = 0 (trabajadores sin pago declarado)
    prop_ing0          = 100 * sum(fac_tri[ingocup == 0], na.rm = TRUE) / sum(fac_tri, na.rm = TRUE),
    
    # Promedio incluyendo ceros (ingreso esperado para cualquier ocupado de ese grupo)
    ingreso_prom_incl0 = weighted.mean(ingocup, fac_tri, na.rm = TRUE),
    
    # Promedio condicional en positivos (ingreso esperado dado que se recibe pago)
    ocupados_pos       = sum(fac_tri[ingocup > 0], na.rm = TRUE),
    ingreso_prom_pos   = if_else(
      ocupados_pos > 0,
      weighted.mean(ingocup[ingocup > 0], fac_tri[ingocup > 0], na.rm = TRUE),
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre, grupo_edad)

print(ingreso_edad_0)
write_csv(ingreso_edad_0, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5g_ingreso_edad0.csv")

# Gráfica: ingreso promedio por grupo de edad, un panel por trimestre
ggplot(ingreso_edad_positivo, aes(x = grupo_edad, y = ingreso_promedio, fill = grupo_edad)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~periodo, ncol = 4) +
  labs(
    title   = "Ingreso promedio por grupo de edad (2022-2025)",
    x       = "Grupo de edad",
    y       = "Ingreso mensual promedio (pesos)",
    caption = "Fuente: ENOE Nueva, ocupados con ingreso positivo"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("grafica_5g_ingreso_edad_positivo.png", width = 14, height = 10, dpi = 300)
cat("  ✓ Gráfica guardada\n")

# ============================================================================
# 5h) DISTRIBUCIÓN DE OCUPADOS POR SEXO
# ============================================================================
# Definición: distribución porcentual de los ocupados (clase2=1) por sexo
#             en cada trimestre.
#
# Codificación de sex: 1=Hombre, 2=Mujer (cualquier otro valor = No especificado)
# El porcentaje se calcula dentro de cada trimestre sobre el total de ocupados.

cat("\n5h) Distribución de ocupados por sexo\n")

distribucion_sexo <- enoe_completa %>%
  filter(clase2 == "1") %>%
  mutate(
    sexo_label = case_when(
      sex == "1" ~ "Hombre",
      sex == "2" ~ "Mujer",
      TRUE       ~ "No especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, sexo_label) %>%
  summarise(ocupados = sum(fac_tri, na.rm = TRUE), .groups = "drop") %>%
  group_by(anio, trimestre, periodo) %>%
  mutate(porcentaje = (ocupados / sum(ocupados)) * 100) %>%
  arrange(anio, trimestre, sexo_label)

print(distribucion_sexo)
write_csv(distribucion_sexo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5h_distribucion_sexo.csv")

# Gráfica: barras agrupadas por sexo a lo largo del tiempo
ggplot(distribucion_sexo, aes(x = periodo, y = porcentaje, fill = sexo_label)) +
  geom_col(position = "dodge") +
  labs(
    title = "Distribución de ocupados por sexo (2022-2025)",
    x     = "Periodo",
    y     = "Porcentaje (%)",
    fill  = "Sexo"
  ) +
  scale_fill_manual(values = c("Hombre" = "#4682B4", "Mujer" = "#DC143C")) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("grafica_5h_distribucion_sexo.png", width = 12, height = 7, dpi = 300)
cat("  ✓ Gráfica guardada\n")

# ============================================================================
# 5i) TASAS DE TRANSICIÓN EMPLEO <-> DESEMPLEO (PANEL ROTATIVO)
# ============================================================================
# La ENOE tiene un diseño de panel rotativo: cada hogar se entrevista durante
# 5 trimestres consecutivos. Esto permite seguir a los mismos individuos en
# el tiempo y calcular probabilidades de transición entre estados laborales.
#
# Metodología:
#   1. Se construye un identificador único de individuo (id_panel) combinando
#      las 11 variables de la clave primaria.
#   2. Se crea una variable de tiempo t = año*4 + trimestre para ordenar.
#   3. Se eliminan duplicados dentro del mismo individuo-trimestre (se conserva
#      la observación del mes más reciente dentro del trimestre).
#   4. Se clasifican los estados: E = Empleado (clase2=1), U = Desempleado (clase2=2)
#   5. Para pares de trimestres consecutivos (t, t+1), se calculan:
#      frac_EU = % de empleados en t que pasan a desempleados en t+1
#      frac_UE = % de desempleados en t que pasan a empleados en t+1
#   6. Todos los cálculos se ponderan con fac_tri.
#
# Implementación en Julia (vía JuliaCall):
#   El procesamiento del panel se delega a Julia por eficiencia. La función
#   crear_enoe_panel_base_v2() recibe el dataframe completo, agrupa por
#   (id_panel, t) y selecciona la observación más reciente por individuo-trimestre.

cat("\n5i) Panel rotativo: transiciones empleo <-> desempleo\n")

# --- Bloque Julia -----------------------------------------------------------
# Inicializar Julia y cargar el paquete DataFrames
julia_setup()
julia_library("DataFrames")

# Definir la función en Julia que procesa el panel ENOE.
# Esta función se ejecuta en el ambiente de Julia (no en R).
julia_command('

function crear_enoe_panel_base_v2(df)
    # Crear identificador único del individuo concatenando las 11 variables de la clave primaria.
    # Este ID permite seguir al mismo individuo a lo largo de los trimestres.
    df.id_panel = string.(df.cd_a, "_", df.ent, "_", df.upm, "_", 
                          df.con, "_", df.d_sem, "_", df.n_pro_viv, "_",
                          df.v_sel, "_", df.n_hog, "_", df.h_mud, "_", 
                          df.n_ren, "_", df.tipo)
    
    # Variable de tiempo t = año*4 + trimestre (ej. 2022Q1 -> 8089)
    # Permite ordenar cronológicamente y verificar consecutividad (t+1 = siguiente trimestre)
    df.t = df.anio .* 4 .+ df.trimestre
    
    # Convertir mes_cal a numérico para seleccionar la observación más reciente
    # en caso de duplicados dentro del mismo individuo-trimestre.
    # Si el valor no es numérico (error de datos), asigna -Inf para que quede al final.
    df.mes_cal_num = map(df.mes_cal) do x
        try
            parse(Float64, string(x))
        catch
            -Inf
        end
    end
    
    # Agrupar por individuo (id_panel) y trimestre (t).
    # Incluimos también anio, trimestre y periodo para conservarlos en el resultado.
    gdf = groupby(df, [:id_panel, :t, :anio, :trimestre, :periodo])
    
    # Para cada grupo (un individuo en un trimestre):
    #   - Contar cuántas filas hay (n_filas, para diagnóstico)
    #   - Seleccionar la observación del mes más reciente (argmax de mes_cal_num)
    result = combine(gdf) do subdf
        n_filas = nrow(subdf)
        max_idx = argmax(subdf.mes_cal_num)   # Índice de la fila más reciente
        row = subdf[max_idx, :]
        (
            id_panel  = row.id_panel,
            anio      = row.anio,
            trimestre = row.trimestre,
            periodo   = row.periodo,
            t         = row.t,
            clase2    = row.clase2,       # Estado laboral
            fac_tri   = row.fac_tri,      # Factor de expansión (ponderador)
            n_filas   = n_filas           # Cuántas filas tenía este individuo-trimestre
        )
    end
    
    # Clasificar el estado laboral en dos categorías:
    #   "E" = Empleado  (clase2 = "1")
    #   "U" = Desempleado (clase2 = "2")
    #   missing = fuera de la fuerza laboral u otro (se excluyen del análisis de transiciones)
    result.estado = map(result.clase2) do c
        if c == "1"
            "E"
        elseif c == "2"
            "U"
        else
            missing
        end
    end
    
    # Seleccionar solo las columnas necesarias para el análisis de transiciones
    result = select(result, :id_panel, :anio, :trimestre, :periodo, :t, :clase2, :fac_tri, :n_filas, :estado)
    
    # Filtrar: mantener solo observaciones con estado válido (E o U) y ponderador no missing
    result = result[.!ismissing.(result.estado) .& .!ismissing.(result.fac_tri), :]
    
    return result
end
')

# Pasar el dataframe de R a Julia (convierte tibble a DataFrame de Julia)
julia_assign("enoe_completa", enoe_completa)

# Ejecutar la función y traer el resultado de vuelta a R
enoe_panel_base <- julia_eval("crear_enoe_panel_base_v2(enoe_completa)")

# Limpiar memoria en Julia
julia_command("enoe_completa = nothing")
julia_command("GC.gc()")   # Equivalente al gc() de R en Julia

# --- Fin bloque Julia -------------------------------------------------------

# Convertir resultado de Julia a tibble de R y asegurar tipos correctos
panel_r <- as_tibble(enoe_panel_base) %>%
  mutate(
    anio      = as.integer(anio),
    trimestre = as.integer(trimestre),
    t         = as.integer(t),
    fac_tri   = as.numeric(fac_tri),
    estado    = as.character(estado),
    id_panel  = as.character(id_panel),
    periodo   = as.character(periodo)
  ) %>%
  # Filtro de seguridad: asegurar que solo haya E o U con ponderador válido
  filter(
    !is.na(estado), estado %in% c("E", "U"),
    !is.na(fac_tri)
  ) %>%
  # Ordenar por individuo y tiempo para facilitar el cálculo de transiciones
  arrange(id_panel, t)

# --- Diagnósticos -----------------------------------------------------------
# Verificar ausencia de duplicados (mismo individuo en el mismo trimestre).
# Si dup_check > 0, hay un problema en la deduplicación de Julia.
dup_check <- panel_r %>%
  count(id_panel, t, name = "n") %>%
  filter(n > 1) %>%
  nrow()

cat("  Diagnóstico: duplicados id_panel-t = ", dup_check, "\n", sep = "")

# Contar cuántas transiciones consecutivas (t -> t+1) hay en los datos.
# n_trans = número de veces que un individuo aparece en trimestres consecutivos.
# Esto mide cuántos "pares" tenemos para el cálculo de transiciones.
trans_count <- panel_r %>%
  group_by(id_panel) %>%
  summarise(n_trans = sum(lead(t) == t + 1, na.rm = TRUE), .groups = "drop") %>%
  summarise(total_trans = sum(n_trans), ids_con_trans = sum(n_trans > 0))

print(trans_count)

# --- Construir pares de transiciones consecutivas ---------------------------
# lead(t) y lead(estado) obtienen el valor del SIGUIENTE trimestre del mismo individuo.
# Filtramos solo los pares donde el siguiente trimestre es efectivamente t+1
# (descarta "saltos" de dos o más trimestres).
transiciones <- panel_r %>%
  group_by(id_panel) %>%
  mutate(
    t_next      = lead(t),        # Siguiente trimestre (cronológico)
    estado_next = lead(estado)    # Estado laboral en t+1
  ) %>%
  ungroup() %>%
  filter(!is.na(estado_next), t_next == t + 1)   # Solo pares consecutivos

# --- Calcular tasas de transición por trimestre -----------------------------
# frac_EU = % de empleados en t que son desempleados en t+1 (tasa de separación)
# frac_UE = % de desempleados en t que son empleados en t+1 (tasa de encontrar empleo)
# Ambas se calculan ponderando por fac_tri.
resultados_5i <- transiciones %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    # Denominadores: total ponderado de E y U en el trimestre t
    E_denom = sum(fac_tri[estado == "E"], na.rm = TRUE),
    U_denom = sum(fac_tri[estado == "U"], na.rm = TRUE),
    
    # Numeradores: flujos de E->U y U->E
    EU = sum(fac_tri[estado == "E" & estado_next == "U"], na.rm = TRUE),
    UE = sum(fac_tri[estado == "U" & estado_next == "E"], na.rm = TRUE),
    
    # Tasas de transición (en porcentaje)
    frac_EU = if_else(E_denom > 0, 100 * EU / E_denom, NA_real_),  # Tasa de pérdida de empleo
    frac_UE = if_else(U_denom > 0, 100 * UE / U_denom, NA_real_),  # Tasa de encontrar empleo
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

cat("\nResultados 5i (panel):\n")
print(resultados_5i)

# --- Verificación de consistencia -------------------------------------------
# Los flujos EE + EU deben sumar el total de empleados (E_denom) en t.
# Si diff_E ≈ 0 y diff_U ≈ 0, la contabilidad es correcta.
# Diferencias no nulas indican individuos con estado válido en t pero
# estado inválido en t+1 (o viceversa), que se pierden al filtrar.
check_flows <- transiciones %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    EE     = sum(fac_tri[estado == "E" & estado_next == "E"], na.rm = TRUE),
    EU     = sum(fac_tri[estado == "E" & estado_next == "U"], na.rm = TRUE),
    UU     = sum(fac_tri[estado == "U" & estado_next == "U"], na.rm = TRUE),
    UE     = sum(fac_tri[estado == "U" & estado_next == "E"], na.rm = TRUE),
    E_denom = sum(fac_tri[estado == "E"], na.rm = TRUE),
    U_denom = sum(fac_tri[estado == "U"], na.rm = TRUE),
    # diff ≈ 0 si todos los individuos con estado E (o U) en t tienen estado
    # E o U en t+1. Diferencias positivas indican "salidas" hacia PNEA.
    diff_E  = E_denom - (EE + EU),
    diff_U  = U_denom - (UU + UE),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(check_flows)

# Exportar resultados finales del panel a CSV
write_csv(resultados_5i, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5i_transiciones_EU_UE.csv")

# ============================================================================
# RESUMEN FINAL
# ============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════╗\n")
cat("║                    ¡EJERCICIO 5 COMPLETO!                         ║\n")
cat("╚═══════════════════════════════════════════════════════════════════╝\n")
cat("\nArchivos generados:\n")
cat("  1. resultados_5b_desempleo.csv\n")
cat("  2. resultados_5c_subempleo.csv\n")
cat("  3. resultados_5d_trabajadores_disponibles.csv\n")
cat("  4. resultados_5e_tamano_empresa.csv\n")
cat("  5. resultados_5e_empresas_pmg.csv\n")
cat("  6. resultados_5f_buscando_empleo.csv\n")
cat("  7. resultados_5g_ingreso_edad_positivo.csv\n")
cat("  8. resultados_5g_ingreso_edad0.csv\n")
cat("  9. resultados_5h_distribucion_sexo.csv\n")
cat(" 10. resultados_5i_transiciones_EU_UE.csv\n")
cat(" 11. grafica_5g_ingreso_edad_positivo.png\n")
cat(" 12. grafica_5h_distribucion_sexo.png\n")
cat(sprintf("\nMemoria final: %.1f MB\n", as.numeric(object.size(enoe_completa)) / 1024^2))
cat("Tiempo estimado: ~5-10 minutos (vs >1 hora anterior)\n\n")
