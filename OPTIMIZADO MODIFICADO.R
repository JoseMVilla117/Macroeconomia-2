# ============================================================================
# EJERCICIO 5 - ENOE NUEVA (2022-2025) - VERSIÓN OPTIMIZADA PARA MEMORIA
# Micro-datos de mercado laboral
# ============================================================================
# Autor: Cortana para Estudiante de Maestría - Colegio de México
# Fecha: 14 de febrero de 2026
# OPTIMIZACIÓN: Libera memoria, solo guarda variables necesarias
# ============================================================================
library(JuliaCall)
library(tidyverse)
library(readr)
library(kableExtra)
library(stargazer)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# RUTA BASE (ajusta según tu sistema)
RUTA_BASE <- "C:/Users/spart/Desktop/Datos Tarea Macro 1"

# Define carpetas de cada trimestre (2022-2025)
carpetas_trimestres <- c(
  # 2022
  "enoe_n_2022_trim1_csv",
  "enoe_n_2022_trim2_csv",
  "enoe_n_2022_trim3_csv",
  "enoe_n_2022_trim4_csv",
  # 2023
  "enoe_n_2023_trim1_csv",
  "enoe_n_2023_trim2_csv",
  "enoe_n_2023_trim3_csv",
  "enoe_n_2023_trim4_csv",
  # 2024
  "enoe_n_2024_trim1_csv",
  "enoe_n_2024_trim2_csv",
  "enoe_n_2024_trim3_csv",
  "enoe_n_2024_trim4_csv",
  # 2025
  "enoe_n_2025_trim1_csv",
  "enoe_n_2025_trim2_csv",
  "enoe_n_2025_trim3_csv"
)

# Mapeo trimestre -> año, trim
info_trimestres <- tibble(
  carpeta = carpetas_trimestres,
  anio = c(rep(2022, 4), rep(2023, 4), rep(2024, 4), rep(2025, 3)),
  trimestre = c(1:4, 1:4, 1:4, 1:3)
)


# ============================================================================
# VARIABLES A CONSERVAR (solo las necesarias) CORREGIDO
# ============================================================================

# Variables clave para el ejercicio 5

vars_clave <- c(
  # Clave primaria (panel / merge)
  "cd_a", "ent", "upm", "con", "d_sem", "n_pro_viv",
  "v_sel", "n_hog", "h_mud", "n_ren", "tipo", "mes_cal",
  
  # Sociodemográficas
  "eda", "sex",
  
  # Laborales
  "clase2", "sub_o", "dispo", "fac_tri", "ingocup", "busqueda",
  
  # Unidad económica / empresa
  "ambito2", "p4a",
  
  # Metadata
  "r_def", "c_res"
)


# ============================================================================
# FUNCIÓN: NORMALIZAR NOMBRES DE COLUMNAS
# ============================================================================

normalizar_columnas <- function(df) {
  names(df) <- str_replace(names(df), "^cve_", "")
  return(df)
}

# ============================================================================
# FUNCIÓN: PROCESAR UN TRIMESTRE (OPTIMIZADO PARA MEMORIA)
# ============================================================================

procesar_trimestre_enoen_optimizado <- function(ruta_carpeta, anio, trimestre) {
  
  cat("\n========================================\n")
  cat(sprintf("Procesando %d Trimestre %d\n", anio, trimestre))
  cat("========================================\n")
  
  # --- PASO 1: Detectar archivos ---
  archivos <- list.files(ruta_carpeta, pattern = "\\.csv$", full.names = TRUE)
  
  sdemt_file <- str_subset(archivos, "SDEMT")
  coe1t_file <- str_subset(archivos, "COE1T")
  
  if (length(sdemt_file) == 0 || length(coe1t_file) == 0) {
    warning(sprintf("Archivos faltantes en %s. Saltando...", ruta_carpeta))
    return(NULL)
  }
  
  cat(sprintf("  SDEMT: %s\n", basename(sdemt_file)))
  cat(sprintf("  COE1T: %s\n", basename(coe1t_file)))
  
  # --- PASO 2: Cargar SOLO las columnas necesarias ---
  cat("  Cargando columnas necesarias...\n")
  
  # Detectar columnas disponibles
  sdemt_cols_all <- names(read_csv(sdemt_file, n_max = 0, col_types = cols(.default = "c")))
  coe1t_cols_all <- names(read_csv(coe1t_file, n_max = 0, col_types = cols(.default = "c")))
  
  # Normalizar nombres
  sdemt_cols_norm <- str_replace(tolower(sdemt_cols_all), "^cve_", "")
  coe1t_cols_norm <- str_replace(tolower(coe1t_cols_all), "^cve_", "")
  
  # Seleccionar solo las que necesitamos
  vars_sdemt <- intersect(vars_clave, sdemt_cols_norm)
  vars_coe1t <- intersect(vars_clave, coe1t_cols_norm)
  
  # Mapear de vuelta a nombres originales
  sdemt_cols_select <- sdemt_cols_all[match(vars_sdemt, sdemt_cols_norm)]
  coe1t_cols_select <- coe1t_cols_all[match(vars_coe1t, coe1t_cols_norm)]
  
  # Cargar SOLO las columnas necesarias
  sdemt_raw <- read_csv(
    sdemt_file,
    col_select = all_of(sdemt_cols_select),
    col_types = cols(.default = "c"),
    locale = locale(encoding = "latin1")
  )
  
  coe1t_raw <- read_csv(
    coe1t_file,
    col_select = all_of(coe1t_cols_select),
    col_types = cols(.default = "c"),
    locale = locale(encoding = "latin1")
  )
  
  # Normalizar nombres
  names(sdemt_raw) <- str_replace(tolower(names(sdemt_raw)), "^cve_", "")
  names(coe1t_raw) <- str_replace(tolower(names(coe1t_raw)), "^cve_", "")
  
  cat(sprintf("  SDEMT: %d filas, %d columnas (reducido desde %d)\n", 
              nrow(sdemt_raw), ncol(sdemt_raw), length(sdemt_cols_all)))
  cat(sprintf("  COE1T: %d filas, %d columnas (reducido desde %d)\n", 
              nrow(coe1t_raw), ncol(coe1t_raw), length(coe1t_cols_all)))
  
  # --- PASO 3: Filtrar SDEMT ---
  cat("  Filtrando población válida...\n")
  
  sdemt_clean <- sdemt_raw %>%
    filter(
      r_def %in% c("0", "00"),
      c_res %in% c("1", "3"),
      as.numeric(eda) >= 15
    ) %>%
    select(-r_def, -c_res)  # Ya no necesitamos estas columnas
  
  rm(sdemt_raw)  # Liberar memoria
  gc(verbose = FALSE)
  
  cat(sprintf("  Población válida: %d personas\n", nrow(sdemt_clean)))
  
  if (nrow(sdemt_clean) == 0) {
    warning("  ⚠️  No hay población válida")
    return(NULL)
  }
  
  # --- PASO 4: Merge (solo clave primaria) ---
  # --- PASO 4: Merge (solo clave primaria) ---
  key_cols_base <- c(
    "cd_a", "ent", "upm", "con", "d_sem", "n_pro_viv",
    "v_sel", "n_hog", "h_mud", "n_ren", "tipo", "mes_cal"
  )
  
  # 1) key_cols se calcula contra coe1t_raw (aún no existe coe1t_clean)
  key_cols <- intersect(key_cols_base, intersect(names(sdemt_clean), names(coe1t_raw)))
  
  # 2) Construir coe1t_clean (puedes dejarlo completo si quieres)
  coe1t_vars <- setdiff(names(coe1t_raw), key_cols)
  coe1t_clean <- coe1t_raw %>%
    select(all_of(c(key_cols, coe1t_vars)))
  
  rm(coe1t_raw)
  gc(FALSE)
  
  # 3) (Opcional pero muy recomendable) quitar duplicados EXACTOS por llave en COE1T
  #    Esto sí hace que el join tenga más chance de ser 1-a-1 sin inventar reglas.
  coe1t_clean <- coe1t_clean %>%
    distinct(across(all_of(key_cols)), .keep_all = TRUE)
  
  cat("  Realizando merge...\n")
  merged <- sdemt_clean %>%
    left_join(coe1t_clean, by = key_cols, suffix = c("", "_coe1"))
  
  rm(sdemt_clean, coe1t_clean)
  gc(FALSE)
  
  # --- PASO 5: Convertir tipos y agregar metadata ---
  cat("  Convirtiendo tipos de variables...\n")
  
  merged <- merged %>%
    mutate(
      # numéricas
      eda     = as.numeric(eda),
      fac_tri = as.numeric(fac_tri),
      ingocup = as.numeric(ingocup),
      
      # categóricas
      clase2 = as.character(clase2),
      sub_o  = as.character(sub_o),
      dispo  = as.character(dispo),
      sex    = as.character(sex),
      
      # nuevas (pueden venir en SDEMT; si no están, queda NA)
      busqueda = if ("busqueda" %in% names(.)) as.character(busqueda) else NA_character_,
      ambito2  = if ("ambito2"  %in% names(.)) as.character(ambito2)  else NA_character_,
      
      # COE1T (si no está, NA)
      p4a = if ("p4a" %in% names(.)) as.character(p4a) else NA_character_,
      
      # identificadores de tiempo
      anio = anio,
      trimestre = trimestre,
      periodo = sprintf("%dQ%d", anio, trimestre)
    ) %>%
    select(-any_of(c("eda_coe1", "fac_tri_coe1")))
  
  
  cat(sprintf("  Datos finales: %d filas, %d columnas\n", nrow(merged), ncol(merged)))
  cat(sprintf("  Memoria usada: %.1f MB\n", as.numeric(object.size(merged)) / 1024^2))
  
  return(merged)
}

# ============================================================================
# EJECUTAR: PROCESAR TODOS LOS TRIMESTRES
# ============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════╗\n")
cat("║         PROCESANDO ENOE NUEVA 2022-2025 (OPTIMIZADO)             ║\n")
cat("╚═══════════════════════════════════════════════════════════════════╝\n")

# Lista para almacenar resultados
lista_trimestres <- list()

for (i in seq_len(nrow(info_trimestres))) {
  carpeta <- info_trimestres$carpeta[i]
  anio <- info_trimestres$anio[i]
  trimestre <- info_trimestres$trimestre[i]
  
  ruta_completa <- file.path(RUTA_BASE, carpeta)
  
  if (!dir.exists(ruta_completa)) {
    warning(sprintf("Carpeta no encontrada: %s. Saltando...", ruta_completa))
    next
  }
  
  # Procesar trimestre
  datos_trim <- procesar_trimestre_enoen_optimizado(ruta_completa, anio, trimestre)
  
  if (!is.null(datos_trim)) {
    lista_trimestres[[length(lista_trimestres) + 1]] <- datos_trim
  }
  
  # Forzar limpieza de memoria cada 3 trimestres
  if (i %% 3 == 0) {
    cat("  → Liberando memoria...\n")
    gc(verbose = FALSE)
  }
}

# ============================================================================
# COMBINAR TODOS LOS TRIMESTRES
# ============================================================================

cat("\n========================================\n")
cat("Combinando todos los trimestres...\n")
cat("========================================\n")

enoe_completa <- bind_rows(lista_trimestres)

# stargazer(enoe_completa)
# 
# kable(head(enoe_completa))

# Liberar lista intermedia
rm(lista_trimestres)
gc(verbose = FALSE)

cat(sprintf("\n✓ Base de datos completa: %d personas, %d columnas\n", 
            nrow(enoe_completa), ncol(enoe_completa)))
cat(sprintf("  Memoria total usada: %.1f MB\n", 
            as.numeric(object.size(enoe_completa)) / 1024^2))

# ============================================================================
# EJERCICIO 5: CÁLCULOS
# ============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════╗\n")
cat("║                      EJERCICIO 5: RESULTADOS                       ║\n")
cat("╚═══════════════════════════════════════════════════════════════════╝\n")

# --- 5b) Tasa de desempleo ---
cat("\n5b) Tasa de desempleo trimestral\n")
cat("\nCalculada como: tasa = 100 * (desocupados / PEA)\n")


# Definí la PEA como el total ponderado (por fac_tri) de personas con clase2 ∈ {1,2} y 
# los desocupados como clase2 = 2. La tasa de desempleo trimestral es 100 × 
# desocupados / PEA, usando fac_tri como ponderador.
# FAC_TRI es el ponderador trimestral (6 dígitos) que indica a cuántas personas
# representa cada caso. 
# Estarías calculando la “tasa” de la muestra, no de la población.
# Y como el diseño muestral no es uniforme, podrías sesgar resultados 
# (sobre/infra-representar estratos).

desempleo <- enoe_completa %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    pea = sum(fac_tri[clase2 %in% c("1", "2")], na.rm = TRUE), #clase2 1 Población ocupada 2 Población desocupada
    desocupados = sum(fac_tri[clase2 == "2"], na.rm = TRUE), # en ambos usamos fac_tri
    tasa_desempleo = (desocupados / pea) * 100,
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(desempleo)
write_csv(desempleo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5b_desempleo.csv")

# --- 5c) Tasa de subempleo ---
cat("\n5c) Tasa de subempleo trimestral\n")
cat("\nCalculada como: tasa = 100 * (subocupados / ocupados)\n")


# Subempleo (subocupación) trimestral. Definí a los ocupados como las personas con 
# clase2 = 1. Definí a los subocupados como aquellos ocupados con sub_o = 1. Para 
# obtener totales poblacionales trimestrales usé el ponderador fac_tri, sumándolo 
# dentro de cada grupo.


# sum(fac_tri[clase2 == "1" & sub_o == "1"], na.rm = TRUE)  “suma esos ponderadores” → 
# eso te da el total poblacional estimado de subocupados

subempleo <- enoe_completa %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    ocupados = sum(fac_tri[clase2 == "1"], na.rm = TRUE),
    subempleados = sum(fac_tri[clase2 == "1" & sub_o == "1"], na.rm = TRUE),
    tasa_subempleo = (subempleados / ocupados) * 100,
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(subempleo)
write_csv(subempleo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5c_subempleo.csv")

# INCISO 5D NO PIDE TASA DE PARTICIPACIÓN LABORAL

# # --- 5d) Tasa de participación laboral ---
# cat("\n5d) Tasa de participación laboral trimestral\n")
# 
# participacion <- enoe_completa %>%
#   group_by(anio, trimestre, periodo) %>%
#   summarise(
#     poblacion_15_mas = sum(fac_tri, na.rm = TRUE),
#     pea = sum(fac_tri[clase2 %in% c("1", "2")], na.rm = TRUE),
#     tasa_participacion = (pea / poblacion_15_mas) * 100,
#     .groups = "drop"
#   ) %>%
#   arrange(anio, trimestre)
# 
# print(participacion)
# write_csv(participacion, "resultados_5d_participacion.csv")



# --- 5d) Trabajadores Fuera de la Fuerza Laboral Disponibles para Trabajar ---

cat("\n5d) Trabajadores Fuera de la Fuerza Laboral Disponibles para Trabajar \n")

cat("\nCalculada como: tasa = 100 * (PNEA disponible / PNEA total)\n")


#La fuerza laboral (labor force) incluye a quienes están participando en el mercado de trabajo, es decir:
  
 # Ocupados (tienen trabajo)

#Desocupados (no tienen trabajo pero están buscando activamente y están disponibles)

#Eso es PEA = ocupados + desocupados.

# Los que no están en esos dos grupos: o sea, no están ni ocupados ni desocupados bajo la definición estándar.

# En la ENOE eso corresponde a la PNEA (no económicamente activa).
# Y dentro de PNEA hay dos subgrupos:
  
  #PNEA disponible: podrían trabajar, pero no cumplen el criterio de “desocupado” porque típicamente no están buscando activamente (o no realizaron acciones de búsqueda).

# PNEA no disponible: no podrían/ no quieren trabajar (estudiantes, hogar, retiro, etc. según codificación).

# “PNEA disponible como proporción de la PEA” (mide “reserva” relativa a la fuerza laboral)

# Muchos enunciados usan “trabajadores” de forma coloquial para referirse a “personas en edad de trabajar”. Técnicamente, en ese inciso no son “trabajadores” (porque no están ocupados), sino personas fuera de la fuerza laboral.

# Definí como “fuera de la fuerza laboral” a la PNEA (clase2 ∈ {3,4}) y como 
# “disponibles” a la PNEA disponible (clase2 = 3). Para cada trimestre calculé la 
# fracción Disponibles / PNEA. Usando como ponderador fac_tri.


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

# --- 5e) Distribución del tamaño de empresa ---
cat("\n5e) Distribución del tamaño de empresa\n")

# micronegocios suma de 2+3

# SIN OTROS, SIN NO ESPECIFICADO

# definimos el tamaño de las empresas primero sumando las categorías sin establecimiento
# con establecimiento de micronegocios. Posteriormente eliminamos la columna 
# de no especificado "otros". 
# tenemos dos bases de datos una con únicamente lo que pide el ejercicio y otra
# que mmantiene a gobierno. 

tamano_empresa <- enoe_completa %>%
  filter(clase2 == "1") %>%
  mutate(
    tamano = case_when(
      ambito2 %in% c("2","3") ~ "Micronegocios",
      ambito2 == "4" ~ "Pequeños_establecimientos",
      ambito2 == "5" ~ "Medianos_establecimientos",
      ambito2 == "6" ~ "Grandes_establecimientos",
      ambito2 == "7" ~ "Gobierno",
      ambito2 == "8" ~ "Otros",
      TRUE ~ "no_especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, tamano) %>%
  summarise(ocupados = sum(fac_tri, na.rm = TRUE), .groups = "drop") %>%
  group_by(anio, trimestre, periodo) %>%
  mutate(porcentaje = (ocupados / sum(ocupados)) * 100) %>%
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
  filter(!tamano %in% c("Otros", "no_especificado")) 

print(tamano_empresa)
write_csv(tamano_empresa, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5e_tamano_empresa.csv")

# SOLO EMPRESAS MICRO PEQUENAS MEDIANAS Y GRANDES

empresas_pmg <- tamano_empresa %>%
  filter(tamano %in% c("Micronegocios", "Pequeños_establecimientos",
                       "Medianos_establecimientos", "Grandes_establecimientos")) %>%
  group_by(anio, trimestre, periodo) %>%
  mutate(porcentaje_pmg = 100 * ocupados / sum(ocupados, na.rm = TRUE)) %>%
  ungroup()

print(empresas_pmg)
write_csv(empresas_pmg, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5e_empresas_pmg.csv")


# --- 5f) Buscando otro empleo ---
cat("\n5f) Proporción de ocupados buscando otro empleo\n")

cat("\nCalculada como: tasa = 100 * (buscando_otro / ocupados)\n")
# solo tomamos a los que sí respondieron

# Definí “trabajadores” como la población ocupada (clase2=1). Identifiqué a quienes 
# buscan otro empleo con busqueda=1. Para cada trimestre calculé el total ponderado 
# de ocupados y de ocupados buscando otro empleo como sumas de fac_tri

buscando_empleo <- enoe_completa %>%
  filter(clase2 == "1") %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    ocupados = sum(fac_tri, na.rm = TRUE),
    buscando_otro = sum(fac_tri[busqueda == "1"], na.rm = TRUE),
    proporcion_buscando = if_else(ocupados > 0, #por si hay ocupados con no especificado
                                  100 * buscando_otro / ocupados,
                                  NA_real_),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

print(buscando_empleo)
write_csv(buscando_empleo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5f_buscando_empleo.csv")

# --- 5g) Ingreso promedio por edad ---
cat("\n5g) Ingreso promedio por grupo de edad\n")

# Usamos promedio ponderado por una razón simple: ENOE es una encuesta, 
# no un censo. Cada fila no representa “1 persona”, sino muchas personas 
# en la población, y eso lo indica fac_tri (factor de expansión/ponderador). 
# Si sacas un promedio simple, estarías promediando la muestra, no a la población.

# Qué hace weighted.mean(ingocup, fac_tri). 


# $$\bar{y}_{w}=\frac{\sum_{i}{w_{i}y_{i}}}{\sum_{i}{w_{i}}}$$
# $$\bar{y}_{w}=\frac{\sum_{i}{fac\_tri_{i}\:ingocup_{i}}}{\sum_{i}{fac\_tri_{i}}}$$

#Así, si una observación representa a 2,000 personas y otra a 200, 
# la primera debe “pesar” más en el promedio poblacional.
# Removimos a los que presentaban salario 0

# Para los ocupados (clase2=1) construí el ingreso promedio por edad usando promedio 
# ponderado con fac_tri. Reporto (i) el promedio condicional en ingresos positivos 
# (ingocup>0) y (ii) la proporción de observaciones con ingocup=0 por grupo de edad,
# para documentar posibles respuestas validas.

ingreso_edad_positivo <- enoe_completa %>% 
  filter(clase2 == "1", !is.na(ingocup), ingocup > 0) %>% 
  mutate( grupo_edad = case_when( eda >= 15 & eda < 25 ~ "15-24",
                                    eda >= 25 & eda < 35 ~ "25-34",
                                    eda >= 35 & eda < 45 ~ "35-44",
                                    eda >= 45 & eda < 55 ~ "45-54",
                                    eda >= 55 & eda < 65 ~ "55-64",
                                    eda >= 65 ~ "65+",
                                    TRUE ~ "no_especificado" ) ) %>% 
  group_by(anio, trimestre, periodo, grupo_edad) %>% 
  summarise( 
    ocupados = sum(fac_tri, na.rm = TRUE),
    ingreso_promedio = weighted.mean(ingocup, fac_tri, na.rm = TRUE),
     n_personas = sum(fac_tri, na.rm = TRUE), .groups = "drop" ) %>% 
  arrange(anio, trimestre, grupo_edad)


print(ingreso_edad_positivo)
write_csv(ingreso_edad_positivo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5g_ingreso_edad_positivo.csv")

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
      TRUE ~ "no_especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, grupo_edad) %>%
  summarise(
    ocupados = sum(fac_tri, na.rm = TRUE),
    
    # % con ingreso 0 (entre ocupados con ingocup no NA)
    prop_ing0 = 100 * sum(fac_tri[ingocup == 0], na.rm = TRUE) / sum(fac_tri, na.rm = TRUE),
    
    # promedio incluyendo ceros
    ingreso_prom_incl0 = weighted.mean(ingocup, fac_tri, na.rm = TRUE),
    
    # promedio solo positivos (condicional)
    ocupados_pos = sum(fac_tri[ingocup > 0], na.rm = TRUE),
    ingreso_prom_pos = if_else(
      ocupados_pos > 0,
      weighted.mean(ingocup[ingocup > 0], fac_tri[ingocup > 0], na.rm = TRUE),
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre, grupo_edad)


print(ingreso_edad_0)
write_csv(ingreso_edad_0, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5g_ingreso_edad0.csv")

# Gráfica
ggplot(ingreso_edad_positivo, aes(x = grupo_edad, y = ingreso_promedio, fill = grupo_edad)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~periodo, ncol = 4) +
  labs(
    title = "Ingreso promedio por grupo de edad (2022-2025)",
    x = "Grupo de edad",
    y = "Ingreso mensual promedio (pesos)",
    caption = "Fuente: ENOE Nueva, ocupados con ingreso positivo"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("grafica_5g_ingreso_edad_positivo.png", width = 14, height = 10, dpi = 300)
cat("  ✓ Gráfica guardada\n")

# --- 5h) Distribución por sexo ---
cat("\n5h) Distribución de ocupados por sexo\n")

distribucion_sexo <- enoe_completa %>%
  filter(clase2 == "1") %>%
  mutate(
    sexo_label = case_when(
      sex == "1" ~ "Hombre",
      sex == "2" ~ "Mujer",
      TRUE ~ "No especificado"
    )
  ) %>%
  group_by(anio, trimestre, periodo, sexo_label) %>%
  summarise(ocupados = sum(fac_tri, na.rm = TRUE), .groups = "drop") %>%
  group_by(anio, trimestre, periodo) %>%
  mutate(porcentaje = (ocupados / sum(ocupados)) * 100) %>%
  arrange(anio, trimestre, sexo_label)

print(distribucion_sexo)
write_csv(distribucion_sexo, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5h_distribucion_sexo.csv")

# Gráfica
ggplot(distribucion_sexo, aes(x = periodo, y = porcentaje, fill = sexo_label)) +
  geom_col(position = "dodge") +
  labs(
    title = "Distribución de ocupados por sexo (2022-2025)",
    x = "Periodo",
    y = "Porcentaje (%)",
    fill = "Sexo"
  ) +
  scale_fill_manual(values = c("Hombre" = "#4682B4", "Mujer" = "#DC143C")) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("grafica_5h_distribucion_sexo.png", width = 12, height = 7, dpi = 300)
cat("  ✓ Gráfica guardada\n")

# --- 5i) Panel rotativo: transiciones empleo <-> desempleo ---
cat("\n5i) Panel rotativo: transiciones empleo <-> desempleo\n")

# julia -------------------------------------------------------------------

# Inicializar Julia (si no lo has hecho ya)

julia_setup()
julia_library("DataFrames")

# Definir la función en Julia que procesa los datos de la ENOE
# Esta función crea el panel base identificando individuos a través del tiempo
julia_command('

function crear_enoe_panel_base_v2(df)
    # Crear identificador único del panel combinando variables de identificación
    # cd_a=código de área, ent=entidad, upm=unidad primaria de muestreo, etc.
    # Este ID permite seguir al mismo individuo entre trimestres
    df.id_panel = string.(df.cd_a, "_", df.ent, "_", df.upm, "_", 
                          df.con, "_", df.d_sem, "_", df.n_pro_viv, "_",
                          df.v_sel, "_", df.n_hog, "_", df.h_mud, "_", 
                          df.n_ren, "_", df.tipo)
    
    # Crear variable de tiempo t = año*4 + trimestre (orden cronológico)
    # Ejemplo: 2020Q1 = 2020*4 + 1 = 8081
    df.t = df.anio .* 4 .+ df.trimestre
    
    # Convertir mes_cal a numérico para ordenar observaciones dentro del trimestre
    # Si falla la conversión (valores no numéricos), asigna -Inf para que queden al final
    df.mes_cal_num = map(df.mes_cal) do x
        try
            parse(Float64, string(x))
        catch
            -Inf
        end
    end
    
    # Agrupar por id_panel y trimestre (t)
    # Esto identifica todas las observaciones del mismo individuo en el mismo trimestre
    gdf = groupby(df, [:id_panel, :t, :anio, :trimestre, :periodo])
    
    # Para cada grupo (individuo-trimestre), calcular n_filas y seleccionar UNA observación
    result = combine(gdf) do subdf
        # Contar cuántas filas hay para este individuo en este trimestre
        n_filas = nrow(subdf)
        
        # Encontrar la observación más reciente (mes_cal_num más alto)
        # Esto resuelve duplicados dentro del mismo trimestre
        max_idx = argmax(subdf.mes_cal_num)
        
        # Retornar la fila seleccionada más el conteo de filas originales
        row = subdf[max_idx, :]
        (
            id_panel = row.id_panel,
            anio = row.anio,
            trimestre = row.trimestre,
            periodo = row.periodo,
            t = row.t,
            clase2 = row.clase2,        # Variable de clasificación laboral
            fac_tri = row.fac_tri,      # Factor de expansión (peso muestral)
            n_filas = n_filas
        )
    end
    
    # Crear variable de estado laboral a partir de clase2
    # "1" = Empleado (E), "2" = Desempleado (U), otros = missing
    result.estado = map(result.clase2) do c
        if c == "1"
            "E"
        elseif c == "2"
            "U"
        else
            missing
        end
    end
    
    # Seleccionar solo las columnas necesarias para el análisis
    result = select(result, :id_panel, :anio, :trimestre, :periodo, :t, :clase2, :fac_tri, :n_filas, :estado)
    
    # Filtrar: mantener solo observaciones con estado válido (E o U) y peso muestral válido
    result = result[.!ismissing.(result.estado) .& .!ismissing.(result.fac_tri), :]
    
    return result
end
')

# Pasar el dataframe de R a Julia para procesamiento
julia_assign("enoe_completa", enoe_completa)

# Ejecutar la función en Julia y traer el resultado de vuelta a R
enoe_panel_base <- julia_eval("crear_enoe_panel_base_v2(enoe_completa)")

# Limpiar memoria de Julia eliminando el dataframe que ya no se necesita
julia_command("enoe_completa = nothing")
julia_command("GC.gc()")  # Forzar recolección de basura

# fin  julia --------------------------------------------------------------

# Convertir el resultado de Julia a tibble de R y asegurar tipos de datos correctos
panel_r <- as_tibble(enoe_panel_base) %>%
  mutate(
    anio = as.integer(anio),
    trimestre = as.integer(trimestre),
    t = as.integer(t),
    fac_tri = as.numeric(fac_tri),
    estado = as.character(estado),
    id_panel = as.character(id_panel),
    periodo = as.character(periodo)
  ) %>%
  # Filtro de seguridad: asegurar que solo tengamos E o U con peso válido
  filter(
    !is.na(estado), estado %in% c("E","U"),
    !is.na(fac_tri)
  ) %>%
  # Ordenar por individuo y tiempo para facilitar cálculo de transiciones
  arrange(id_panel, t)

# 2) (Opcional) Diagnósticos rápidos

# Verificar que no haya duplicados (mismo individuo en mismo trimestre)
# Esto debería ser 0 si la función de Julia trabajó correctamente
dup_check <- panel_r %>%
  count(id_panel, t, name = "n") %>%
  filter(n > 1) %>%
  nrow()

cat("  Diagnóstico: duplicados id_panel–t = ", dup_check, "\n", sep="")

# Contar cuántas transiciones consecutivas (t -> t+1) existen en los datos
# n_trans = número de veces que un individuo aparece en trimestres consecutivos
trans_count <- panel_r %>%
  group_by(id_panel) %>%
  summarise(n_trans = sum(lead(t) == t + 1, na.rm = TRUE), .groups="drop") %>%
  summarise(total_trans = sum(n_trans), ids_con_trans = sum(n_trans > 0))

print(trans_count)

# 3) Construir transiciones consecutivas t -> t+1
# lead() obtiene el valor del siguiente trimestre para cada individuo
transiciones <- panel_r %>%
  group_by(id_panel) %>%
  mutate(
    t_next = lead(t),              # Siguiente trimestre
    estado_next = lead(estado)     # Estado laboral en el siguiente trimestre
  ) %>%
  ungroup() %>%
  # Mantener solo pares consecutivos (t y t+1) donde ambos estados sean observados
  filter(!is.na(estado_next), t_next == t + 1)

# 4) Calcular tasas de transición por trimestre
# frac_EU = % de empleados en t que pasan a desempleados en t+1
# frac_UE = % de desempleados en t que pasan a empleados en t+1
# Todas las cifras son ponderadas con fac_tri (factor de expansión)
resultados_5i <- transiciones %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    # Denominadores: total de empleados (E) y desempleados (U) en t
    E_denom = sum(fac_tri[estado == "E"], na.rm = TRUE),
    U_denom = sum(fac_tri[estado == "U"], na.rm = TRUE),
    
    # Numeradores: transiciones de E->U y U->E
    EU = sum(fac_tri[estado == "E" & estado_next == "U"], na.rm = TRUE),
    UE = sum(fac_tri[estado == "U" & estado_next == "E"], na.rm = TRUE),
    
    # Calcular porcentajes de transición
    frac_EU = if_else(E_denom > 0, 100 * EU / E_denom, NA_real_),
    frac_UE = if_else(U_denom > 0, 100 * UE / U_denom, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(anio, trimestre)

cat("\nResultados 5i (panel):\n")
print(resultados_5i)

# Verificación adicional: calcular TODOS los flujos posibles entre estados
# Esto permite verificar que los flujos sumen correctamente (EE+EU debe = E_denom)
check_flows <- transiciones %>%
  group_by(anio, trimestre, periodo) %>%
  summarise(
    # Flujos de E->E, E->U, U->U, U->E (ponderados)
    EE = sum(fac_tri[estado=="E" & estado_next=="E"], na.rm=TRUE),
    EU = sum(fac_tri[estado=="E" & estado_next=="U"], na.rm=TRUE),
    UU = sum(fac_tri[estado=="U" & estado_next=="U"], na.rm=TRUE),
    UE = sum(fac_tri[estado=="U" & estado_next=="E"], na.rm=TRUE),
    
    # Totales de empleados y desempleados en t
    E_denom = sum(fac_tri[estado=="E"], na.rm=TRUE),
    U_denom = sum(fac_tri[estado=="U"], na.rm=TRUE),
    
    # Verificar consistencia: la suma de flujos debe igualar los totales
    # diff_E y diff_U deberían ser ≈0 si todo está correcto
    diff_E = E_denom - (EE + EU),
    diff_U = U_denom - (UU + UE),
    .groups="drop"
  ) %>%
  arrange(anio, trimestre)

print(check_flows)

# Exportar resultados finales a CSV
write_csv(resultados_5i, "C:/Users/spart/Desktop/Datos Tarea Macro 1/Resultados Optim/resultados_5i_transiciones_EU_UE.csv")



# ============================================================================
# RESUMEN FINAL
# ============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════╗\n")
cat("║                    ¡EJERCICIO 5 COMPLETO!                         ║\n")
cat("╚═══════════════════════════════════════════════════════════════════╝\n")
cat("\nArchivos generados:\n")
cat("  1-8. resultados_5b-5i.csv (8 archivos)\n")
cat("  9-10. graficas PNG (2 archivos)\n")
cat(sprintf("\nMemoria final: %.1f MB\n", as.numeric(object.size(enoe_completa)) / 1024^2))
cat("Tiempo estimado: ~5-10 minutos (vs >1 hora anterior)\n\n")
